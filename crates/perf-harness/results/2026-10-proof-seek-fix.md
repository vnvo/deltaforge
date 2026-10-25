# Proof seek fix: acceptance results, October 2026

The MySQL source's activation proofs (`docs/design/mysql-proof-seek.md`,
revisions 2 to 5) against the release before the fix, on one host. The
sensitivity series (`2026-10-proof-seek-sensitivity.md`) measured the
defect; this records the fix's acceptance, including two criteria that
failed and were withdrawn as invalid measurements, with their runs kept.
These are single-instance results, not a fleet qualification.

## Result

Activation latency no longer depends on the binary log's offset. At 256
MiB, the 200 first-touched tables' p99 fell from about 51 s to 177-217 ms;
at 960 MiB from about 54 s to 181-229 ms. Seek is flat at 0.24-0.60 ms per
scan. No row was missing, duplicated or reordered in any run, and the
instruction count is at parity.

## Conditions

- **Point:** 1,000 tables, 20% active (200 tables first touched per run),
  2,000 single-row inserts a second for 50 s, binary log padded with small
  single-row transactions to 0, 64, 256 or 960 MiB before each run.
- **Systems:** the fix at `26a3b893` (production code; later commits add
  only trace records and harness counters) and the release before it
  (`runner-avro4`). SQLite state store, Kafka JSON sink.
- **Host:** Intel i7-1355U laptop (2 performance and 8 efficiency cores,
  15 W), shared by MySQL, Kafka, the load generator and the runner. CPU
  seconds there are bimodal for the same binary, so instructions are the
  CPU measure.

## Final series (`26a3b893`, 24 runs)

Raw results: `target/perf/final-26a3b893`; evaluation script fixed
(`sha256 7d6cd062...`) before the run.

| Offset | Activation p99, fix | Activation p99, before | Seek p50, fix | Scans for 200 tables, fix / before |
|---|---|---|---|---|
| 0 | 301-370 ms | 3.3-8.1 s | 0.54-0.60 ms | 7-10 / 200 |
| 64 MiB | 177-227 ms | about 21 s | 0.24-0.32 ms | 7-8 / 200 |
| 256 MiB | 177-217 ms | about 51 s | 0.25-0.35 ms | 8-10 / 101 |
| 960 MiB | 181-229 ms | about 54 s | 0.25-0.28 ms | 8-12 / 31 |

Every fix run: zero unanchored scans; one capture attempt per window, at
most two stream openings and two extensions; every table activated inside a
window; the window-level activation trend (Theil-Sen over per-window median
latency against its size bucket) between -160.8 and -2.7 ms over the run,
never upward. Offset 0 ran slower for both systems in this series than in
earlier ones (about 200 ms and about 3 s); the machine was loaded at its
start.

Latency by window size, reported apart: one-table windows p50 82-272 ms;
2-16 tables p50 109-271 ms; 65-256 tables p50 151-342 ms, p99 167-370 ms.

## Withdrawn criteria

Each measured something other than the activation path; the runs stay
recorded as failed.

1. **Activation-decile ratio within 2x.** It divided the latency of the
   burst's large windows (about 100-130 tables sharing one capture) by that
   of late single-table windows. Late windows do less work, so the ratio
   rewarded making them slower: an adaptive close that shortened them made
   the ratio worse while improving every table's latency. Replaced by the
   window-level trend and by reporting window sizes apart.
2. **Within-window delivery, last-admitted minus first-admitted, at most
   50 ms** (final series: 5 windows at 52.6-85.5 ms). Every violating
   window's first rows arrived in two or three bursts 39-42 ms apart. The
   trace ties each burst to one delivery batch: in 23 windows spanning two
   batches, rows arrived within 0-3.7 ms of each other inside a batch and
   37.7-42.9 ms apart between batches, matching the batches' delivery-end
   gaps of 40.1-42.8 ms. The spread is the downstream batch cycle, after
   the window's commit.
3. **Release skew, last-admitted minus first-admitted, at most 5 ms**
   (diagnostic `release-diag-294d7de5`: 16 of 35 windows at 21-38 ms). A
   window's tables are admitted atomically, then its held events are handed
   to the runner one at a time in binary-log order. The last-admitted table
   is first touched later in the log, so its first row sits behind hundreds
   of earlier events. Releasing it sooner would mean reordering events or
   decoding the backlog instantly; neither is a requirement.

## The ordered-drain contract

What the window must do after its commit, from the same diagnostic (6 runs
at 0 and 256 MiB, `294d7de5`, trace records of every window commit, table
release and delivered batch):

- **Strict binary-log order:** no reordered row in any run.
- **No release stall above 5 ms:** the longest pause between consecutive
  releases was 3.31 ms, over 1,150 gaps.
- **Drain at least 10x the offered rate:** held events drained at
  25,546-390,840 events/s against 2,000/s offered (at least 12.8x).
- **The held queue drains completely:** every row of every run delivered.
- **Reported:** 2.6-39.1 us per released event; at most 1,420 events in one
  window's queue and 1,771 held during one admission (bound: 10,000).

Activation p99 in that diagnostic: 188-224 ms at 0, 203-224 ms at 256 MiB.

## Instructions

`perf stat` user-space instructions over 30 s of steady load, summing the
hybrid CPU's two core types (`--perf-counters`):

| Comparison (offset 0, 3 interleaved pairs) | Ratios | Median | 95% upper bound |
|---|---|---|---|
| Same binary on both sides (control) | 0.954, 0.971, 1.010 | 0.971 | 1.052 |
| Fix against the release before | 1.034, 0.991, 1.028 | 1.028 (limit 1.05) | 1.078 (limit 1.10) |

The control shows the measurement's noise is about 5%.

## Downstream observation

Delivered batches on this pipeline complete about every 40 ms under this
load, so first rows released together can arrive up to one batch cycle
apart. That belongs to the batching path, not to proof seeking.

## Not adopted

Measured on the experiment branch `exp-window-diag` (kept, never merged),
six repetitions each at 0 and 256 MiB:

- **Adaptive close** (a window also closes after 10 ms without a new
  table): worst decile ratio 2.36 against 2.10 current, with 54% more
  windows and captures. It shortened small windows only.
- **Parallel schema reads** (three reader sessions beside the lock holder,
  each checking its locks through `performance_schema.metadata_locks`):
  probe 10 (`evidence/mysql-proof-seek/` on that branch) confirmed the
  locking and failure argument, but under this load the schema step's p95
  fell only from 39.0 to 35.8 ms, against the 25 ms required, for four
  connections per capture and a new `performance_schema` grant.
