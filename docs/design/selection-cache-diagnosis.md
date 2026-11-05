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
| `deltaforge_perf_selection_store_reads_total` | `read` = `history` \| `history_prefetch` \| `versions` \| `version` | One **logical loader call** a miss caused (section 2.1): timelines a row needed, timelines a window read ahead, the batched version read-ahead, one version a selection needed. From the selection-cache fix on, a `history` or `history_prefetch` call is one load: it reads one statement's tables at a time and may stop before the last, and its version read-ahead is one `versions` call per statement. |
| `deltaforge_perf_selection_store_tables_total` | `read` | Tables (or versions) those calls asked the store for. From the selection-cache fix on, a load that stops once nothing more fits counts only the tables of the statements it issued; before the fix a load read, and counted, every table it was given, so the two counts mean the same (`perf_a_stopped_load_counts_the_tables_it_requested`). |
| `deltaforge_perf_selection_store_bytes_total` | `read` | For `history` and `history_prefetch`: the timeline-byte accounting of the timelines the call returned (section 2.1). Zero for `versions` and `version`. |
| `deltaforge_perf_phase_latency_seconds` | `phase` = `history_load` \| `history_prefetch` \| `version_prefetch` \| `version_load` (the last three new) | Elapsed time of each call (existing histogram; `history_load` existed). Nesting in section 2.2. |
| `deltaforge_perf_selection_cache_limit` | `bound` = `tables` \| `bytes` | The bounds the cache was built with (`bytes` alone from the selection-cache fix on). |

A timeline miss always causes one `history` call (with other tables batched in); a selection miss causes a `version` call only when its version is not cached.

### 2.1 What the store counters do not measure

- **Calls, not backend reads or SQL statements.** A `history` or `history_prefetch` call is one `mysql_history::read_each` (from the selection-cache fix on, one per statement of its load), which issues bounded batches of at most `BATCH_MAX_TABLES` tables plus the barrier streams not yet read, and reads alone a table its batch could not take whole. A `versions` call is one `get_versions`: two batched backend reads, plus one more read for each version that is not its stream's latest. A `version` call is one `get_version`: an index lookup and a log read. The number of backend reads or statements per call is not counted.
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
- **`--scale-arm NAME=BINARY[:tables=N][:bytes=N]`** (repeated; not with `--scale-cache-tables`): the DeltaForge runs are these arms, each on its `--binary` with only its own bounds, once per repetition, interleaved, the order reversed in odd repetitions. Every DeltaForge binary must run through an arm and every label must be distinct. The run's tag and `version` are the arm's name; `runs.jsonl` records the arm (`arm`: name, binary, bounds) and the binary's label and SHA-256. Before a run measures, the runner must report every bound its arm set (`deltaforge_perf_selection_cache_limit{bound}`; a build bounded by bytes alone reports no table bound, so a table bound on it fails); otherwise the run fails as an invocation error. A bound an arm leaves out is the build's own and is recorded as reported.
- **Environment isolation**: a runner never inherits the harness's measurement settings. Every `DELTAFORGE_PERF_*` variable of the harness's environment is removed from the runner's and only its run's own are set (`a_runner_gets_only_its_runs_measurement_settings`). Every launch of a run's runner, the first and any relaunch, is built by the same command with the same environment (`a_relaunch_keeps_the_runs_settings`). The scaling study does not relaunch runners: its catch-up phase stops and resumes the pipeline in the same process, so the process's environment and bounds are unchanged.
- **Memory**: each DeltaForge run records its peak RSS (`rss_peak_mib`) and its peak anonymous resident memory (`rss_anon_peak_mib`, `RssAnon`: the allocator's heap, freed pages it keeps included, and thread stacks; not the mapped binary or files).
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

## 6. Fix validation plan (for review; nothing has run)

The fix (branch `selection-cache-byte-budget`, PR #195) removes the 4,096-timeline bound. The cache is bounded by 256 MiB of estimated memory alone, the estimate counts what entries allocate at jemalloc's size classes, and a load admits at most half the bound (its first timeline may take all of it) without evicting its own entries. Its measurement build takes only the byte bound (`DELTAFORGE_PERF_SELECTION_CACHE_BYTES`); the table variable of section 3 applies to `df89d2d0` builds only.

### 6.1 Assumptions, revised for full accounting

Accurate accounting changes what fits. The fully accounted working set of a point, estimated with the cache probe for the harness's four-column tables (one to three records per timeline, each table with its selection and version; approximate, since the probe's columns carry more description than the harness's), is about 11.6 to 13.3 KB per active table:

| Active tables | Working set (estimate) | Against 256 MiB |
|---|---|---|
| 200 | about 2 MiB | fits |
| 2,000 | about 22 MiB | fits |
| 5,000 | about 55 MiB | fits |
| 8,000 | about 88 MiB | fits |
| 20,000 | about 221 to 254 MiB | at the bound |

The 20,000-table burst is at the bound, and its burst load (about 232 MB) exceeds what one load may admit (128 MiB), so part of a burst window is left uncached and read again when reached, without any eviction. Evictions and rereads at the default bound where the working set does not fit are remaining capacity pressure, not an implementation defect: they are reported (rule 6), not judged against the no-eviction reference. No-eviction latency is required only where the working set fits with room for any single load (rule 1).

The working set is measured, not assumed, where it decides the rule. A measurement of W counts only when it is complete and churn-free: the arm that measured it shows zero evictions and zero timeline reloads after the last activation. A peak gauge alone can understate a working set that is already churning, so a churning arm never classifies a point.
- At the 20,000-table point, W is measured by an arm with a bound above it (F-L, 4 GiB): its peak cache-bytes gauge.
- At the other points, W is F's own peak gauge, valid only if F is churn-free there.
- If no churn-free measurement exists, or W exceeds 128 MiB, the point's classification returns for review before any rule is applied.

Classes, by W. The 128 MiB line is a conservative classification for these tests, not a general guarantee that every load fits:
- **Fits** (a complete, churn-free W of at most 128 MiB, half the budget): in these tests the working set and every load fit. Rule 1 applies.
- **At the bound** (128 MiB < W at most 256 MiB): it fits at rest but not in one load. Rule 6 reports it; whether F's post-activation evictions stay within B-L's plus 1 per 1,000 rows (fits at rest) is reported as a capacity finding, not a pass or fail.
- **Over the budget** (W above 256 MiB, and F-64 everywhere it runs): byte pressure, reported by rule 6.

### 6.2 Builds

From one clean checkout of the PR head `<H>`, into `target/perf/diag-builds-<H>/`, as in section 5: `default/runner`, `perf-phases/runner` (both `cargo build --release -p runner`, the second with `--features perf-phases`), `canonical-perf` and the harness, and `build-identity.txt` (revision, clean-tree check, commands with features, SHA-256 of every binary). The base is the pinned `df89d2d0` measurement runner (`target/perf/diag-builds-df89d2d0/perf-phases/runner`, SHA-256 `97767855...`). Every invocation uses the harness built at `<H>` and its `--scale-arm` support (section 4).

### 6.3 Step F0: instrumentation overhead at the fix revision

The overhead result of section 5 step 1 was measured at `df89d2d0` and does not transfer: accounting and admission changed what the instrumentation counts (per eviction, per refused load, per statement). Arms `D=fixdefault` (default build) and `P=fixdiag` (measurement build), both at the code's bounds, interleaved in one invocation with alternating order, 5 repetitions, 240 s load, one warm-up per arm per fixture (the harness warms up on each fixture's first point):
- t1000-a20-burst-o0 (200 active: no evictions);
- t10000-a50-steady-o0 (5,000 active: no evictions expected now);
- t40000-a50-burst-o0 (20,000 active: at the bound, so admission refusals and possibly evictions occur).

30 runs. Overhead is compared only on metrics both builds export: post-activation p50 and p99, CPU, and evictions (the default build's `deltaforge_mysql_selection_cache_evictions_total`). The default build has none of the diagnostic counters (timeline reloads, store reads, phase timings); their absence there is not zero, and they are not compared. Tolerances as in section 5 step 1 (P minus D: p50 at most +3% or +1 ms, p99 at most +3% or +2 ms, CPU at most +5%, evictions within 2%). The same one-sided and anomaly rules apply. Outside tolerance at any point: the overhead is reported and the plan returns for review before any run of step F1.

**Result at `84dfd735` (2026-11-05): F0 failed.** All 30 runs completed with no missing or duplicate rows. At t40000-a50-burst-o0, the deferral-only runs passed under the earlier ruling. Two metrics fell outside tolerance:
- At t40000-a50-burst-o0, p99 was +2.34 ms (tolerance 2.0).
- At t40000-a50-burst-o0, evictions were 190 for D against 218 for P (tolerance 3.8). Every P run was above every D run.

Everything else was within tolerance. A read-only analysis of the retained runs (`~/deltaforge-evidence/f0-review-84dfd735`) found that the measurement build changes behavior:
- At t10000-a50-steady-o0, in 4 of 5 runs, it activates tables 40 to 60% later, forms about a third fewer windows, and roughly triples the activation-period p99.
- At the bound, its eviction total is higher with no shared observable of occupancy, admission or windowing to explain it.

The verdict stands. Step F1 is therefore split (section 6.4.1): acceptance is judged on default builds, and the measurement builds only attribute.

### 6.4 Step F1: comparison

**Arms** (`--scale-arm`; each sets its bounds explicitly, so every bound is verified before a run measures):
- `B=base:tables=4096:bytes=268435456`: the base at its default bounds.
- `B-L=base:tables=32768:bytes=268435456`: the base with no evictions (a table bound above every active set), the old code's no-eviction reference.
- `F=fix:bytes=268435456`: the fix at its default bound.
- `F-L=fix:bytes=4294967296`: the fix with a bound above the working set (20,000-table point only): measures W and is the fix's own no-eviction reference.
- `F-64=fix:bytes=67108864`: the fix under byte pressure (20,000-table point only).

**Points**, one invocation per point (arms are per invocation), fresh state per run, 5 repetitions, 240 s load, one warm-up per arm, order alternating:

| Point | Store | Active | W estimate | Class | Arms |
|---|---|---|---|---|---|
| t10000-a50-steady-o0 | SQLite | 5,000 | 55 MiB | fits | B, B-L, F |
| t40000-a20-steady-o0 | SQLite | 8,000 | 88 MiB | fits | B, B-L, F |
| t10000-a50-burst-o0 | PostgreSQL | 5,000 | 55 MiB | fits | B, B-L, F |
| t40000-a50-burst-o0 | SQLite | 20,000 | 221-254 MiB | measured (F-L) | B, B-L, F, F-L, F-64 |
| t1000-a20-burst-o0 | SQLite | 200 | 2 MiB | control | B, F |
| t10000-a20-burst-o0 | SQLite | 2,000 | 22 MiB | control | B, F |

90 runs plus 18 warm-ups.

**Metrics**, per run, with the interval rules of section 4.1 and the 80-second sufficiency rule (a pair with fewer than three sufficient runs gets no conclusion):
- post-activation latency p50 and p99 (outside catch-up), CPU cores;
- per 1,000 delivered rows: evictions, timeline reloads, single version loads, read-ahead time, store reads and tables requested (section 2: the counts mean the same for both builds);
- cold fill: time to the last activation, activation-period non-first-row p99, store reads and tables requested before the last activation;
- memory, in three layers that are not interchangeable:
  1. **Cache-owned allocated memory**: the peak of the cache-bytes gauge. For F it is the estimate the memory tests validate against the entries' allocations (0.78 to 0.99 of it, jemalloc's allocated bytes). For B it is the old estimate, which undercounts selections and versions: reported, never compared.
  2. **The allocator's resident memory** cannot be read: jemalloc's statistics are compiled out of both builds (`config.stats: false`, checked on the pinned base runner), and enabling them for the fix alone would change the allocator under test. Its proxy is the runner's peak anonymous resident memory (`rss_anon_peak_mib`). That includes pages jemalloc keeps after frees: the memory tests show resident memory staying near its peak after eviction until those pages decay. It also includes thread stacks.
  3. **Process RSS**: `rss_peak_mib`.
- correctness, frontier, resource and deferral classification as in steps 2 and 3.

**Acceptance**, fixed before running (medians of sufficient runs):
1. **Fits, F against B-L:** p50 within +3% or +1 ms, p99 within +3% or +2 ms, CPU within +5% (the step 1 tolerances); evictions and timeline reloads per 1,000 rows at most B-L's plus 1. Where the working set fits, remaining churn is a defect: a failure is reported with its counters and the plan returns for review.
2. **Fits, direction:** F's evictions and post-activation p50 are lower than B's (a sanity check; rule 1 decides).
3. **Controls, F against B:** p50, p99 and CPU within the rule 1 tolerances; evictions zero in both.
4. **Cold fill, F against B, every point except F-64's:** time to the last activation within +5% or +1 s, whichever is larger; activation-period non-first-row p99 within +5% or +5 ms, whichever is larger; store reads before the last activation at most B's, tables requested at most B's plus 2%. Activation interference stays a separate open limitation; this rule only requires that the fix does not worsen cold fill.
5. **Memory:** F's cache-bytes gauge never exceeds its bound in any sample (F-L 4 GiB, F-64 64 MiB). At every point with a B-L arm, F's peak anonymous resident memory is at most B-L's plus 10%: B-L holds every active table, so its memory is what holding them costs, whatever its gauge says. The two control points have no B-L arm: there B is the memory reference (no evictions occur in either build), and F's peak is at most B's plus 10%. F-64's is at most F's. Process RSS is reported for every arm.
6. **At the bound and over the budget** (t40000-a50-burst-o0: F against F-L and B; F-64 against F-L, F and B): no latency threshold. Reported: W, evictions, timeline reloads, rereads of tables a burst left uncached, latency, CPU and memory, as the churn that remains when the working set exceeds what the budget or one load holds.
7. **Invalidation:** section 6.5.

Any rule that fails is reported with its data; no acceptance is claimed from a partial result.

#### 6.4.1 Split after F0 (revised 2026-11-05; supersedes the arms and rules above where they differ)

The F0 result (section 6.3) shows the measurement build is not a faithful stand-in for the default build during activation or at the bound. Cache-bound overrides exist only in measurement builds, so a default build runs only at its code's bounds. Step F1 is therefore two parts, and no comparison crosses them.

**Activation** is when a table's first row arrives (section 4.1). The harness's `activated_ms` in `phases.json` is when the workload started writing to the table. The analysis loads it under the name `workload_start_ms` and never uses it for an activation boundary.

**Part A, acceptance, default builds only.**
- **Arms:** `B=base` (the base's default runner, at its code bounds of 4,096 tables and 256 MiB) and `F=fix` (the fix's default runner, at 256 MiB). No overrides.
- **Points:** the six points above, one invocation each, 5 repetitions, 240 s load, one warm-up per arm, order alternating. 60 runs plus 12 warm-ups.
- **Rule 1:** unavailable in Part A. A default build has no no-eviction reference, and F improving on the count-limited B does not establish equivalence to one.
- **Rule 2 (direction)** at the three fits points: F's post-activation evictions per 1,000 delivered rows and its post-activation p50 are each lower than B's.
- **Rule 3 (controls)** as above.
- **Rule 4 (cold fill), every point:** the time to the last activation, and the activation-period non-first-row p99, against the thresholds above. The activation-period p99 covers non-first rows committed at or before the last activation, outside catch-up. Store reads and tables requested are unavailable in default builds and move to Part B.
- **Rule 5 (memory):** F's cache-bytes gauge never exceeds 256 MiB in any sample. The cache-owned allocation budget is validated by that gauge together with the accounting and memory tests.
  - B at its default holds fewer tables than F, so it is not the same-working-set reference that the +10% criterion needs.
  - B's and F's process RSS and peak anonymous RSS are reported, with no threshold applied.
  - Process-memory acceptance stays open pending review.
- **Rule 6** as above, for t40000-a50-burst-o0 (F against B only).
- **Sufficiency and intervals:** section 4.1 and the 80-second rule apply to the post-activation metrics. Cold-fill metrics use every completed run. Failures, rejected intervals and insufficient runs are reported with their values, never dropped.

**Part B, attribution, measurement builds only.**
- **Arms:** the arms above on the measurement builds (the base's `97767855`, the fix's perf-phases runner), compared only with each other.
- **Rule 1** is judged here, F against B-L, explicitly as a result about the measurement builds. It cannot establish default-build performance near capacity.
- **Loader calls** move here: store reads, tables requested, timeline reloads and version loads, before and after the last activation. Each result states F0's instrumentation limitation.
- **Version reads:** the one single version read per activated table (seen at every F0 point) is reported separately from recurring post-activation loads.
- Part B does not repair or supersede F0's verdict.
- **Arms per point** (14 arm-points, 5 repetitions, one warm-up per arm: 70 runs plus 14 warm-ups): B-L and F at the three fits points; B-L, F, F-L and F-64 at t40000-a50-burst-o0; B and F at the two controls. Every arm sets its bounds explicitly, as in section 6.4.
- **Definitions:** a timeline reload is a timeline requested by a history or history_prefetch loader call (`deltaforge_perf_selection_store_tables_total`) after the last activation, outside catch-up. Row-time lookup misses are reported separately and are not reloads, since read-ahead hides most of them. Store reads count every loader call (history, history_prefetch, versions, version), and tables requested count the history and history_prefetch tables. "Before the last activation" is each counter's cumulative value from the runner's start to the first sample after the last activation. Each estimate records the closing sample's times.
- **Correction (2026-11-06, before any Part B result):** the frozen verdict script was written with timeline lookup misses as the reload measure. It runs unchanged and its output is kept. A supplement applies the definition above, with the tolerances unchanged.
- **Catch-up:** the steady workload's stop and resume restarts the source, and its restart path reloads what it needs. That span is reported on its own, so restart reloads are not counted as post-activation churn.
- **Rule 4's loader criteria** (store reads before the last activation at most B's; tables requested at most B's plus 2%) are judged at the two controls, where Part B has a B arm. At the other points Part B has no B arm, and F against B-L is reported with the same criteria, not judged.
- **Rule 5:** each F arm's gauge stays within its bound in every sample. Process RSS and peak anonymous RSS are reported for every arm, with F against B-L, and no threshold is applied while process-memory acceptance stays open.

### 6.5 Invalidation cost (release build)

Debug-build timings (a database barrier 13 ms, a lineage barrier 405 ms, at 120,000 entries) establish nothing about operational cost. The cost is measured in a release build of `<H>` on the measurement host with nothing else running, by `invalidation_cost_at_the_default_bound` (`cargo test --release -p sources --lib -- --ignored --nocapture invalidation_cost_at_the_default_bound`). The test fills the default bound with its population of the smallest real tables, each with a one-record timeline and a one-column selection and version, over 100 databases: 191,571 entries (63,857 tables) before the index (section 7), 201,324 entries (67,108 tables) with it, since a table's string is now held once for its entries. That is the tested population, not a universal maximum: other entry mixtures hold other counts. It then times each operation on a full cache, five times, in each of six populations: the entries in the order they were cached, and the same entries with the LRU order reshuffled first (every entry used once in a random order, as reuse reshuffles it) for the fixed seeds 1 to 5. The criteria apply to each population, and every population's timings are reported. Two more populations put 90% of the tables in one database (in insertion order, and seed 1). They also time a barrier on that database, and are reported for review, not judged against the criteria. `large_values_at_the_default_bound` times one 64 MiB timeline (the per-timeline limit, with records that really hold that much) being replaced, evicted, and refused by admission, together with the drop of a refused value. Those pauses are reported separately.

The insertion-order population alone, as first planned, missed reordered reuse. Measured on scratch builds of the code before the index (`~/deltaforge-evidence/invalidation-optionA-4294cf99`), a reshuffled LRU order took a database barrier from about 4 ms to about 22 ms and a lineage barrier from about 67 ms to about 150 ms: their per-entry path walked the whole LRU list. The baseline already failed there.

Criteria, revised 2026-11-05 (provisional validation budget): a database barrier at most 5 ms median, with its maximum at most 1.5 times its median; a lineage barrier and a full clear each at most 100 ms median and 150 ms maximum, at the default 256 MiB bound. They apply to the insertion-order and every reshuffled population. They are measured acceptance targets for these populations, not worst-case guarantees. The earlier criteria (50 ms medians for the whole-cache operations) are superseded: meeting them took background reclamation (section 8), and no operational requirement justified that complexity. A barrier runs on the source's task and delays its stream once.
- A database barrier follows DDL in one database, which is frequent across a fleet, so it must stay an order of magnitude below the no-eviction post-activation p99 (46 ms at t40000-a20-steady).
- A lineage barrier (a stream start or lineage change) and a full clear (a restart or failover) are rare, and each must stay within about one such p99.

All five timings of each operation are reported, with any scheduling anomaly (such as one timing far above the others, with the host's load at the time). A failure is reported as measured and never rerun away; the plan returns for review.

Exactness is covered by the unit and memory tests: the accounting, the shared streams, and memory released after a full clear.

Results so far: at `df726153` and `f4d2141c` (insertion order only), a database barrier passed (4.0 and 4.3 ms) and a lineage barrier (66.5, 67.5 ms) and a full clear (61.6, 63.5 ms) failed (`~/deltaforge-evidence/invalidation-cost-df726153`, `-f4d2141c`). Both are recorded as a failed operational-cost check; the criteria are unchanged.

At `84dfd735`, under the revised criteria, every population passed (`~/deltaforge-evidence/selection-cache-timing-84dfd735`):
- database barrier medians 1.38 to 1.75 ms, with each maximum at most 1.08 times its median;
- lineage barrier medians 69.5 to 70.4 ms, maximum 71.0 ms;
- full clear medians 68.5 to 70.6 ms, maximum 71.4 ms.

The concentrated 90% database barrier took 121.4 and 123.9 ms (medians). It is a documented limitation at a nearly full cache, provisionally accepted: it does not meet the ordinary 5 ms database-barrier target, and frequent broad invalidations remain unqualified.

### 6.6 Order, time and disk

Order: the release-build invalidation measurement, then F0, then F1 only if F0 is within tolerance. F0 failed (section 6.3). After review, F1 runs as Part A (section 6.4.1), whose results return before Part B.

Time: about 4 minutes per run, start to start (step 3: 242 s), so F0 (30 runs and 6 warm-ups) takes about 2 hours and F1 (90 runs and 18 warm-ups) about 7 hours, plus the fixture builds.

Disk, measured on the step 3 outputs:
- Each run keeps 96 to 239 MB, so the 144 runs and warm-ups need about 14 to 33 GB.
- The builds of `<H>` need about 4 GB in the measurement checkout's cold `target/release`, which was removed, and 0.2 GB of pinned binaries.
- The release test build needs about 3 GB in the fix worktree.

Each invocation starts with at least 50 GiB free and runs under the harness's 25 GiB floor.

## 7. The barrier index

A barrier on stream `s` invalidates every entry whose table key starts with `s`. Before the index, it found them by walking every entry of the cache, and a table's invalidation (at every install) did too. The cache now keeps its keys by database and table (`mysql_cache_index.rs`):
- A table key is `v/tenant/source/lineage/db/table` with every segment percent-encoded, so its database's barrier stream is the key up to its last `/`. Under database stream `d`, a barrier on `s` applies to every entry when `d` starts with `s`, to those whose table key starts with `s` when `s` starts with `d`, and to none otherwise. Visiting the databases (one map entry each) therefore finds exactly what a scan finds; a randomized test checks this against a scan, with deeper and segment-cutting prefixes.
- A barrier that applies to every entry is a full clear.
- Each table's keys are a vector, and each entry keeps its position there, so a removal swaps the last key into its slot and tells the moved key's entry: a table with a long history costs no more per removal than a small one. A vector a quarter full or less (of at least 16) is shrunk to twice its keys.
- A table's string is held once, shared by its keys and the index.

The index's memory is in the estimate and the bound: the databases' and tables' strings (once each), the maps' buckets (tracked from their capacity after each insert; they shrink only when dropped) and the tables' vectors at capacity. Admission counts what indexing an entry may add (an upper bound: a map with no room left is taken to resize). `the_index_allocates_its_estimate` checks the estimate against jemalloc exactly, through fills and removals, and the cache's memory probe stays at 0.78 to 0.99 of the estimate with the index.

Its upkeep is measured by `upkeep_at_the_default_bound` (release build): filling, admitting with eviction, single-table invalidation, and a table with 20,000 versions cached, evicted one at a time and invalidated.

## 8. Reclamation: designed, implemented and set aside

Freeing invalidated entries away from the source's loop was designed (three revisions) and implemented: stamps for currency, a ring of deferred values, bounded steps, and a nonblocking handoff to a reclamation thread. It was reverted for synchronous ownership with the barrier index (section 7):
- Its core gate (`622c88c9`) was red. A load's evictions shared one inline allowance, so under eviction pressure a load cached few of the tables it had read: 322 reads for 300 tables returning to a 512 KiB cache (`mysql_history_cache_e2e`), where at most 75 are allowed.
- Fixing that needed calibrated allowances and batch sizing. That still could not help when entries larger than the inline limit dominate: in a test model, every returning table was read on its own.
- The whole-cache pauses it removed (about 70 ms at the default bound, measured) are rare. They are now accepted under the revised budget in section 6.5.

What stays is the synchronous design: an invalidation removes and frees its entries before it returns, so no entry is ever stale, the bound counts only what the cache holds, and the install check (the same cached value) is sufficient. The work is kept on the local branch `reclamation-preserved-622c88c9`, bundled in `selection-cache-reclamation-preserved`, with the red gate in `gate-selection-cache-622c88c9`.


## 9. Returning tables: a table is admitted with its version

The core gate on `9b6045b1` (the code of `6b0ac60f`) failed `evicted_tables_return_in_batched_reads`: 119 store reads for 300 tables returning to a 512 KiB cache, where at most 75 were allowed. The same test, run alone and alternating on `f4d2141c` (before the index) and `9b6045b1`, gave 17 to 70 and 21 to 78 reads: the count depended on how much of the burst the stream had read ahead when the first table was reached. Those runs do not establish that the index caused no regression.

The test now holds the source (the same run and cache) at the first event of the burst until all of it is in the binlog, then releases it, so every load can read as many tables as its batch allows. Held that way, both revisions read history in a few batches but each table's version alone: 277 single-version reads on `f4d2141c` and 242 on `9b6045b1`, an index lookup and a log read each. The cause predates the index:
- A load admitted its timelines first and read the versions they name afterwards, only if the load could still admit something.
- Batches were sized from the held entries' table bytes. Under a backlog a batch's timelines filled the load's half of the cache, so the versions were never read, and each table's selection then read its own.

The fix:
- A load reads a batch's histories, then the versions their last proofs name in one batch, for the tables whose timelines alone still fit what the load may admit (a floor of each charge, so every table that can be admitted is among them).
- Each table is admitted with its version as one charge, reserved before either is cached: the entries, their keys and indexing (the version's index growth is taken before the timeline indexes the table, an upper bound) and the barrier streams not held. A timeline is never cached without room for its version.
- At the first table that does not fit, it and the rest of the batch are read but not cached; each is read again, in a batch, when reached. Later statements of the load are not read. A table asked for is served whether or not it was cached.
- A batch is sized by the larger of the held tables' average bytes and the largest charge the last load admitted, so a load caches every table it reads. Without that, batches of 37 cached 34 tables and each left 3 to a batch of their own (17 batches for 300 tables).

On `9b6045b1` with the fix, the held burst takes 9 batched history reads and 36 store calls (from 500 to 566), every batch caches every table it reads, and no version is read alone. The test derives its ceiling from the largest charge any batch weighed: a load filling half the bound caches at least `fill = floor(bound / 2 / largest) - 1` tables, so the burst needs at most `ceil(300 / fill)` batches (10). It checks the assumptions behind that instead of trusting them: every batch but the last read at least `fill` tables, a batch that left tables uncached cached at least its room over the largest charge less one, every history read is a batch's, and no version is read alone. With batching disabled (a batch room of 1) the test fails with 300 single-table reads. A live burst after the held one is checked for exact delivery and the bound only.
