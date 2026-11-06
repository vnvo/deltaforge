# Selection cache byte budget: validation results, November 2026

The MySQL source's selection cache bounded by 256 MiB of accounted memory
instead of 4,096 timelines (PR #195, design
`docs/design/selection-cache-diagnosis.md`, section 6), measured at
`84dfd735` against the base `df89d2d0` on one host. The scoped performance
validation is accepted with the qualifications below. This is not a release
acceptance, and the failed checks stay recorded as failed.

## Result

Where the active set exceeded the old 4,096-timeline bound, the fix removed
the eviction churn. At 20,000 active tables (burst), the default build ran
195 evictions against 1,113,493, and post-activation p50 fell from 73.6 to
28.7 ms and p99 from 141.1 to 78.4 ms. The last activation came 18.8 s
earlier and the activation-period p99 fell from 22.7 to 7.4 s. Where the
working set fits, the fix matched the base. In all 160 measured runs (F0,
Part A and Part B), no row was missing or duplicated.

Qualifications: instrumentation overhead (F0) failed, so measurement-build
results do not transfer to default builds; one cold-fill loader check
failed and is accepted as an exception (E1); the 20,000-table working set
was not established under the frozen criterion (E2); and acceptance covers
the cache's accounted allocation, not process RSS.

## Conditions

- **Points:** six, each 5 repetitions of 240 s load with one warm-up per arm,
  arms alternating, fresh state per run.

  | Point | Store | Active tables | Class |
  |---|---|---|---|
  | t10000-a50-steady-o0 | SQLite | 5,000 | fits |
  | t40000-a20-steady-o0 | SQLite | 8,000 | fits |
  | t10000-a50-burst-o0 | PostgreSQL | 5,000 | fits |
  | t40000-a50-burst-o0 | SQLite | 20,000 | at the bound |
  | t1000-a20-burst-o0 | SQLite | 200 | control |
  | t10000-a20-burst-o0 | SQLite | 2,000 | control |

- **Builds:** pinned in `target/perf/diag-builds-84dfd735` (default runner
  `d6a0632f`, measurement runner `40c55abe`, harness `f4d00701`, release
  test `b7cf6231`) and `target/perf/diag-builds-df89d2d0` (default runner
  `24703414`, measurement runner `97767855`).
- **Activation** is a table's first row arriving. Medians of sufficient runs
  (at least 80 usable post-activation seconds).

## Invalidation cost (release build)

Every population passed the revised budget: database barrier medians 1.38
to 1.75 ms (maximum at most 1.08 times the median), lineage barrier medians
69.5 to 70.4 ms (maximum 71.0), full clear medians 68.5 to 70.6 ms (maximum
71.4). A database barrier over 90% of a nearly full cache took 121.4 and
123.9 ms (medians). That is provisionally accepted as a documented
limitation: it does not meet the 5 ms database-barrier target, and frequent
broad invalidations remain unqualified.

## F0: instrumentation overhead (failed)

30 runs, default against measurement build of `84dfd735`, all complete and
correct. At t40000-a50-burst-o0, p99 was +2.34 ms (tolerance 2.0) and
evictions 190 against 218 (tolerance 3.8). At t10000-a50-steady-o0 the
measurement build activated tables 40 to 60% later in 4 of 5 runs. The
verdict stands; acceptance was judged on default builds (Part A), and the
measurement builds only attribute (Part B).

## Part A: acceptance, default builds (60 runs)

All 60 runs complete, correct and sufficient (usable intervals 99 to 224
s); 19 failed only on activation-window deferrals. Frozen verdict: no
failure. The no-eviction comparison (rule 1) is unavailable in default
builds.

| Point | Evictions per 1,000 rows, B / F | p50, B / F | Last activation, B / F | Activation p99, B / F |
|---|---|---|---|---|
| t10000-a50-steady | 537.4 / 0 | 52.8 / 26.4 ms | 20.18 / 21.05 s | 3.51 / 3.30 s |
| t40000-a20-steady | 1455.3 / 0 | 59.2 / 26.5 ms | 66.65 / 52.84 s | 21.3 / 20.7 s |
| t10000-a50-burst | 521.6 / 0 | 217.1 / 27.9 ms | 20.14 / 20.40 s | 5.26 / 5.25 s |
| t40000-a50-burst | 2375.1 / 0.20 | 73.6 / 28.7 ms | 105.14 / 86.36 s | 22.7 / 7.4 s |
| t1000-a20-burst | 0 / 0 | 27.96 / 27.97 ms | 0.29 / 0.27 s | 190.8 / 190.4 ms |
| t10000-a20-burst | 0 / 0 | 27.96 / 27.93 ms | 5.69 / 5.67 s | 1092.9 / 1030.6 ms |

The tightest margin: at t10000-a50-steady-o0 the last activation was 0.87 s
later, against an allowance of 1.01 s. F's cache gauge never exceeded 256
MiB.

## Part B: attribution, measurement builds (70 runs)

Arms: B-L (base with a 32,768-table bound, its no-eviction reference) and F
at the fits points; B-L, F, F-L (4 GiB) and F-64 (64 MiB) at the 20,000-table
point; B and F at the controls. All 70 runs complete, correct and
sufficient (usable 99.3 to 224.2 s); 26 failed only on deferrals. B-L is a
diagnostic-build reference: nothing here supersedes F0 or establishes
default-build performance.

**No-eviction equivalence (rule 1), F against B-L: passed** at all three
fits points: p50 within -0.18 to +0.05 ms, p99 within -0.01 to +0.05 ms,
CPU within +0.002 cores, zero evictions in both. Timeline reloads are
counted as timelines requested by history loader calls after the last
activation, outside catch-up (requests, not proven installs or physical
store reads): 0.005 to 0.132 per 1,000 rows, within B-L's plus 1. The frozen
verdict, written with lookup misses as the measure, also passed; both are
kept.

**Cold-fill loader calls (rule 4), F against B at the controls:**
t1000-a20-burst passed (203 against 203 calls, 200 tables each).
t10000-a20-burst **failed**: 2,387 against 2,349 logical loader calls
before the last activation (medians, +38, 1.6%), with 2,000 tables requested
in every run of both arms. See exception E1.

**At the bound (t40000-a50-burst):** F ran 0.054 evictions per 1,000 rows,
F-L none, and F-64 2,193 with p50 77.6 ms. Its working set W is not
established (exception E2).

## Exceptions accepted for this validation

**E1, cold-fill loader calls at t10000-a20-burst.** The +38 logical-call
median difference is accepted. Every run requested the same 2,000
timelines; runs differ in how many arrived by activation-window read-ahead
and how many by on-demand loads. Both arms show the same two patterns
(single-table on-demand calls without deferrals, and 1.4 to 2.4 tables per
call with deferrals), and F's on-demand calls carried more tables (median
2.03 against 1.44). The extra 8.7 ms of measured loader time per cold span
(37.3 against 28.5 ms; 0.098 against 0.099 ms per call) produced no observed
cold-fill latency or CPU regression: last activation -15 ms, activation p99
-27.5 ms, cold-span CPU -0.02 s. These are logical loader calls, not
physical store statements. The frozen failure stays recorded; no code was
changed to make the counter pass.

**E2, the working set at 20,000 tables.** The frozen criterion requires an
arm with zero evictions and zero timeline requests after the last
activation. F-L had zero evictions but requested 1 to 29 timelines per run
(B-L, far below its bound, 4 to 24), nearly all through read-ahead, each
with a selection miss and no timeline lookup miss. That supports no
capacity churn; an invalidation-and-reload cause is plausible but not
proven (no invalidation counter, no per-table logs). W is recorded as not
established, and no sizing claim depends on it.

## Memory limitation

Acceptance covers the validated cache-owned allocation accounting: the
cache-bytes gauge stayed within each bound in every sample, and the
accounting and memory tests check it against the allocator. It does not
cover process RSS. In default builds at 20,000 active tables, the fix's peak
RSS was about 150 MiB higher than the base's (1,034.8 against 885.1 MiB;
anonymous 1,005.3 against 851.5). The base retained at most 4,096 timelines
and the fix 20,000; that difference is context, not a proven attribution of
the whole increase. In the measurement builds, against B-L holding the same
20,000 tables, the peaks were 997.6 (F) and 1,010.2 MiB (B-L) anonymous;
that does not transfer to default builds.

## Open items

- F0's failed verdict stands.
- An unexplained SIGBUS: the CI coverage job's sources library test binary
  crashed once at `df726153`, then passed on rerun and on later revisions.
- The broad-invalidation pause above (121 to 124 ms) is provisional.
- Activation interference is a separate open limitation.
- Single host; not a fleet qualification.

## Evidence

`~/deltaforge-evidence/`: `selection-cache-timing-84dfd735`,
`f0-overhead-84dfd735` and `f0-review-84dfd735`, `part-a-<point>-84dfd735`
and `part-a-review-84dfd735`, `part-b-<point>-84dfd735` and
`part-b-review-84dfd735` (frozen verdict, supplement, memory and cold-read
analyses), each with a verified SHA-256 manifest.
