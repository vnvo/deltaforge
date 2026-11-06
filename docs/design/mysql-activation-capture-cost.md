# MySQL activation: schema-capture cost

Status: **step 1 done; step 2 authorized; validation criteria final
(2026-11-06).** The probe's findings are below. The query change (step 2)
is authorized; step 3, the product A/B, is held until the implementation is
reviewed, and runs against the criteria fixed here. Deeper speculation,
reordered delivery and release acceptance are held.

## Problem

While tables activate, rows of tables that are already active wait. The
source emits in strict binary-log order from one task, so every row behind
an activation window waits for that window's commit, and windows commit one
after another. Only the next window's capture and scan overlap the current
one.

From the Part A default-build traces of PR #195
(`~/deltaforge-evidence/activation-interference-analysis-84dfd735`):

- Capture is the longest phase. At the 40,000-table points it summed to 61
  to 65 s per run over an activation span of 52 to 86 s (phases overlap;
  the sums are not additive). Commit took 3 to 6 s, scan about 1 s and
  read-ahead under 1 s.
- About a third of capture is the existence query plus the metadata locks;
  about two-thirds is the four schema queries. Schema cost per table was 2
  to 6 ms, against about 1 ms measured on small catalogs
  (`mysql-mass-invalidation.md`). It was higher at the steady points than
  at the burst points of the same catalog size.
- The runner used 0.22 to 0.36 cores during activation: it was waiting on
  the server.

Every capture statement on `INFORMATION_SCHEMA` filters by a row
constructor, `(TABLE_SCHEMA, TABLE_NAME) IN ((?, ?), ...)`. Why the
statements cost what they do is not established; that is what the probe
measures.

## Constraints

Kept as they are: strict binary-log order, checkpoint monotonicity, nothing
emitted past an undecided earlier row, the source task's synchronous
ownership of the selection cache, batched captures only (no per-table round
trips), read-only speculation at most one window ahead, one read-only
transaction per capture with its locks taken in sorted order, and the
capture's results and failure behavior.

## Step 1: offline query probe (authorized)

A standalone program on the product's MySQL driver (`mysql_async` 0.36.2,
default options, prepared statements) against a throwaway server of the
harness's image, digest and settings. No product code changes.

- **Fixtures:** the harness's table definition at 10,000 and 40,000 tables,
  in two layouts: the harness's 100 tables per database, and every table in
  one database. One fixture at a time, so each catalog stands alone. A small
  equivalence fixture (varied types, keys, prefixes, collations, case,
  engines, a view, tables without a key) is present in every catalog.
- **Forms:** the current statements verbatim, and rewrites of the
  `INFORMATION_SCHEMA` filters (one `TABLE_SCHEMA = ? AND TABLE_NAME IN
  (...)` branch per database; a join against a derived table of the
  requested pairs) and of the collation lookup (a join instead of the
  per-row subquery). Each form runs the whole capture sequence inside the
  capture's transaction and lock sequence: session lock wait, `START
  TRANSACTION READ ONLY`, existence, sorted locks, position, schema reads,
  position, `COMMIT`.
- **Controls:** the same requested tables, batch sizes and parameters for
  every form, in rotated order; the same server settings; the same
  concurrent-write load (the steady workload's 2,000 rows/s on 64 writers
  into the batches' active set), and runs without it.
- **Retained per sample:** every statement's client time (prepare and
  execute), server time, CPU and rows examined (performance schema), the
  container's CPU, and the plan of every statement form at every batch size.
- **Warm and cold:** warm samples after warm-up on a running server; cold
  samples after a server restart with the buffer-pool dump and reload off.
  Reported separately. The host's page cache stays warm (a limitation).
- **Equivalence:** for every sample, each form's rows are compared with the
  current statements' rows: values and their types, nulls, order where the
  product depends on it (columns in rank and ordinal order, key columns in
  ordinal order), keys, prefixes, collations, and result metadata. Batches
  with missing tables check missing-table behavior. Any difference
  disqualifies the form.
- **The existence query:** measured within each form, never removed. Its
  role (missing-table detection before locking; a table dropped after it
  fails the attempt) is assessed separately in the findings.
- **Bounds:** the 25 GiB free-disk floor is checked throughout; a breach
  stops the probe. The program, fixture definitions, server version and raw
  results are retained with a manifest.

### Findings (2026-11-06)

Evidence: `~/deltaforge-evidence/capture-probe-7724ff08` (manifest
`f499c1a5`), MySQL 8.4.9.

- **Cause.** The row-constructor filter is never used for a lookup: each
  statement reads every table of the catalog (or of the database, when all
  tables share one) and filters afterwards. Each statement costs about the
  same at any batch size: about 30 ms at 40,000 tables, 55 ms under the
  steady load. At 64 tables that is 4.3 ms per table, as in the Part A
  traces.
- **The derived-table join** (`FROM (VALUES ROW(?, ?), ...) AS r (s, n)
  JOIN <view> ON TABLE_SCHEMA = r.s AND TABLE_NAME = r.n`) reads tables by
  name in every plan, at every size and in every catalog. Non-lock capture
  time at 64 tables, steady load: 57 to 15 ms at 10,000 tables, 274 to 22
  ms at 40,000 (3.0 to 3.7 and 11.4 to 12.5 times). No gain at one table.
- **Equivalence.** Values, types, nulls, order, keys, prefixes, collations
  and result metadata identical to the current statements in every
  comparison, including case and accent pairs, missing tables and
  databases, a view, MyISAM, partitioned tables and prefix, composite and
  descending keys. The per-database and per-pair `UNION` forms change
  result metadata and are slower; a join instead of the collation subquery
  gains nothing. Neither is used.
- **Locks are separate.** A batch's first capture opens its tables while
  locking (about 0.25 to 0.6 ms per table) in every form.
- **The existence query** costs 1 to 2 ms warm under the join (9 to 14 ms
  cold). It stays: removing it would turn a dropped table into a failed
  batch (1146 on its lock) instead of a missing one.
- **Limits.** One capture at a time (production overlaps windows and runs
  up to four capture slots); the host page cache stayed warm; 8.4.9 only.

## Step 2: query change (authorized)

The five capture statements (existence, columns, primary key, key prefixes,
table metadata) filter by the derived-table join, with bound parameters.
Unchanged: the existence query, the collation subquery, `FIELD` rank order
and every `ORDER BY`, the transaction, the sorted lock order and failure
handling. The window, speculation and commit code is untouched. The
requested pairs are deduplicated (first occurrence kept), so every caller
gets the rows it got from `IN`.

`VALUES ROW` needs MySQL 8.0.19: the source refuses an older server, and
MariaDB, before it reads anything. That is the minimum the statements need;
qualification remains MySQL 8.4 (the probe measured 8.4.9 only).

Equivalence is proven by the existing capture and schema-loader tests plus
a live test comparing every statement's rows and result metadata with the
previous row-constructor statements.

## Step 3: validation (provisional)

Default builds only, so the failed instrumentation check (F0 of PR #195)
does not apply. Arms: B = `7724ff08`, F = the change. Five repetitions per
arm at each of the five points below: 50 measured runs, plus one warm-up per
arm and point. Arms alternate, state is fresh per run, 240 s of load, the
Part A sufficiency rule (at least 80 usable post-activation seconds).

Points:

| Point | Role |
|---|---|
| t10000-a50-steady-o0 | improvement |
| t40000-a20-steady-o0 | improvement |
| t40000-a50-burst-o0 | improvement |
| t1000-a20-burst-o0 | no-regression control |
| t10000-a20-burst-o0 | no-regression control |

Definitions:

- **Activation** of a table: the arrival of its earliest-committed row in
  the run. The **last activation** is the latest over the run's tables.
- **Already-active row:** a row of table T, other than T's
  earliest-committed row, committed at or after T's activation. Rows of T
  committed before T's activation wait for T's own window and are excluded.
- **Activation-period already-active latency:** arrival minus commit over
  the already-active rows committed at or before the last activation; p50
  and p99 per run, medians over runs.
- **Capture cost per table:** the sum of the trace records' `capture_ms`
  over the sum of their `tables`, per run (default builds emit these
  records).
- **Post-activation latency:** as in Part A, excluding the restart
  catch-up.

Criteria, fixed before any run. Percentages apply to the medians of the
five runs per arm. A miss returns the result for review: it never triggers
a rerun or a revised threshold.

1. **Correctness, every run:** no missing, duplicate or out-of-order row,
   no run error. Any failure fails the validation.
2. **Mechanism, improvement points:** capture cost per table at least 50%
   lower at t40000-a20-steady-o0 and t40000-a50-burst-o0, and at least 30%
   lower at t10000-a50-steady-o0.
3. **Outcome, improvement points:** the last activation and the
   activation-period already-active p99 each at least 20% better at both
   40,000-table points and at least 10% better at t10000-a50-steady-o0;
   and every F run better than B's median on both.
4. **No regression, controls:** the last activation within +5% or +1 s,
   and the activation-period already-active p99 within +5% or +5 ms,
   whichever is larger (the Part A cold-fill tolerances); capture cost per
   table at most B's plus 10% or 0.2 ms, whichever is larger.
5. **Guard, every point:** post-activation p50 within +3% or +1 ms, p99
   within +3% or +2 ms, CPU within +5% (the Part A tolerances); deferrals
   and MySQL connections reported.

The probe's gain is statement time. Window overlap and locking determine
how much of it reaches activation latency; only criterion 3 measures that.
