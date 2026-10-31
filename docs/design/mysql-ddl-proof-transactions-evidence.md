# MySQL DDL forward-proof transactions: validation evidence

Evidence for the C1-C3 implementation described in
[mysql-ddl-proof-transactions.md](mysql-ddl-proof-transactions.md), measured
with the `actdiag` harness (`crates/perf-harness/actdiag/`) under the rule of
section 7, which was fixed before any result was seen.

Builds (release, rustc 1.97.1; identities in each series' `invocation.json`):

| label | revision | binary sha256 | perf-phases |
|---|---|---|---|
| old | f0171b4e (before the dense-catalog DDL work) | c786930b... | backported, patch 44088a75... |
| main | 3ff3773c | 8850efc2... | backported, patch 6ea4c747... |
| change | d9310d14 (this branch; later commits touch only the harness and docs) | 5a102ec6... | native |

Every run starts fresh MySQL and state-store servers on tmpfs. A run counts
only when the drain is authoritative: every sink checkpoint's `gtid_set`
contains the final `gtid_executed` and the ledger of written rows equals
the consumed rows, within the drain timeout.

## Retained series

Each series is copied to `$HOME/deltaforge-evidence/<series>/` and verified
against its manifest after it completed and before any of its resources
were removed.

| series | runs | failed | incorrect | MANIFEST.sha256 sha256 |
|---|---|---|---|---|
| overhead-20261030 | 36 | 0 | 0 | 0370995e10951ddead6b41355631d802e4d389500a646ce5b3998df497e214d6 |
| pg-ddl-20261030 | 27 | 0 | 0 | c63022dbcaeac8c3dd83f21abb5beaf485c4586b2766d5398c1239fcabcc4000 |
| sqlite-act-20261030 | 27 | 0 | 0 | 85aa8a8ed9faed0fea578d6eaa8a7665db4966779d072c27f302693a28ddd346 |

The analyzer fails on an incomplete series. That means a missing build,
comparison or metric, fewer repetitions than the series was started with
(never fewer than 3), or no judged metric. All three series were
re-analyzed from their retained copies with it. Each is complete, and every
verdict is identical to the original analysis. The re-analysis is retained
as `reanalysis-20261031`.

## Instrumentation overhead (section 6)

`ddl-unloaded` and `ddl-loaded` on PostgreSQL, each revision built with and
without `perf-phases`, 3 repetitions, judged with `--max-diff 0.03`. Every
end-to-end metric of every revision is "no worse" within 3%, so the
instrumented builds are used for the comparisons below.

## DDL path on a PostgreSQL store

3 repetitions per build; every metric of `change` against `old` is
"no worse". Medians:

| comparison | metric | old | change | main |
|---|---|---|---|---|
| ddl-unloaded (100 tables, 300 CREATE) | ms per DDL | 12.06 | 10.80 | 12.54 |
| ddl-loaded (1000 tables, 150 busy at 2000 rows/s, 1000 DDLs) | ms per DDL | 15.99 | 15.02 | 18.04 |
| | busy-table lag p99 | 13.76 s | 12.81 s | 15.79 s |
| ddl-burst (150 busy, 4200 CREATE) | ms per DDL | 19.3 | 18.0 | 21.1 |
| | busy-table lag p99 | 71.6 s | 66.1 s | 79.0 s |

State store per DDL during the DDL phase (`pg_stat_statements`, all store
traffic):

| build | explicit commits | statements | store-lock calls |
|---|---|---|---|
| old | 3 | 39.5 | 3 |
| main | 11 | 59.5 | 8 |
| change | 4 | 32.5 | 2 |

The change commits one write transaction per forward proof (plus the
earlier ddl record, C2); its extra commits over old are the read-only
freshness reads of C3.

Phase distributions (diagnostic, not judged) explain the difference:

- main's regression against old is in registration (`register` +1.35 to
  1.6 ms per DDL) and in the timeline reload after it (`history_load`
  +0.4 to 0.5 ms).
- the change replaces `register` and `bind` with one `plan` and one
  `commit`, and has no `history_load`: C3 installs the proven entries into
  the timeline the loader prefetched before the DDL, after the two
  freshness reads (`fresh_check`, 0.6 to 0.8 ms).
- `store_pool_wait` is negligible in every build, so pool contention is
  not a cause.

## Activation on a SQLite store

200 tables at 400 rows/s, 3 repetitions per build; every metric of
`change` against `old` is "no worse". Medians:

| comparison | metric | old | change | main |
|---|---|---|---|---|
| act-fresh | activation p50 / p99 | 73.7 / 111.4 ms | 76.8 / 110.0 ms | 76.6 / 110.9 ms |
| act-burst | activation p50 / p99 | 150.9 / 157.5 ms | 153.3 / 161.0 ms | 158.1 / 167.2 ms |
| act-restart | activation p50 / p99 | 26.7 / 46.2 ms | 36.2 / 60.7 ms | 36.5 / 60.4 ms |

act-restart is about 10 ms slower at p50 and 14 ms at p99 than old, inside
the 50 ms absolute tolerance. main shows the same difference and the change
matches main, so it predates this change; it comes with main's restart
path taking no `history_load` where old takes 200. It is recorded as an
observation, not attributed to C1-C3.

## Observations

- The 2x burst gap of the earlier report is not reproduced by this harness.
  Its burst is a backlog of DDLs, where old and main differ by about 9%
  (ms per DDL) and the change is below both.
- Repeated ALTERs of the same table under backlog stop old (f0171b4e) with a
  pending-DDL fail-closed: the known failed-forward-proof follow-up. The
  loaded workload therefore alters distinct, warmed, idle tables in every
  build.
- A timeline installed by the activation window counts fewer bytes than the
  same timeline loaded from the store. This predates the change; the C3
  tests compare the snapshot's bytes plus its two installed entries.

## Mutation check

9 mutants in a fresh worktree at 53af90e9: 7 killed (M1, M2, M5, M6, M8,
M10, M7b) and 2 judged equivalent:

- M7 (install, report false): the caller invalidates every key that is
  not reported installed, so the end state is identical.
- M9 (snapshot on a replayed ddl record): the replayed record's sequence is
  at or below the cached head, so the freshness check rejects it.
