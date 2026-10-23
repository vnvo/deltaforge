# Proof seek against binary-log offset: sensitivity series, October 2026

Pins the cost of DeltaForge's MySQL proof seek
(`docs/design/mysql-proof-seek.md`) against the binary log's starting
offset. Measured as declared in `scale-study-design.md` (Binary-log
position, and Amendments), on one host. These are single-instance results,
not a fleet qualification.

## Conditions

- **Point:** 1,000 tables, 20% active (200 tables first touched in each
  run), 2,000 single-row inserts a second for 50 s, no catch-up phase.
- **Systems:** DeltaForge `cd716061`; Debezium 3.6.3.Final, tuned, one
  connector over every table.
- **Starting offset:** before each run the binary log is flushed and the
  fresh file padded to the starting offset, in a database neither system
  captures. Padding:
  - **small:** single-row transactions, the shape of an OLTP server's
    binary log;
  - **large:** 8 MiB rows (few, large events). It understates the seek and
    is shown for contrast.
- **Load-generation settings**, equal for both systems and not production
  recommendations: 64 writer connections; the source does not sync its
  logs (`sync_binlog=0`, `innodb_flush_log_at_trx_commit=2`).
- **Repetitions and correctness:** 3 repetitions per point, interleaved;
  42 runs, all successful. No file rotated during a run. No row was
  missing, duplicated or out of order. Every run measured all 200 first
  rows.
- **Raw results:** `target/perf/sensitivity2` (large padding) and
  `sensitivity2-small` (small), harness `720ccafb`, clean tree. They
  include each run's commit and arrival times (`rows.csv`).

## Results (medians over 3 repetitions)

Activation latency is the latency of each table's first row. Steady is
that of every other row.

| Padding | Offset | DeltaForge seek per proof p50 / p95 | Physical scans of 200 proofs | DeltaForge activation p50 / p95 / p99 | DeltaForge steady p99 | Debezium activation p50 / p95 / p99 | Debezium steady p99 |
|---|---|---|---|---|---|---|---|
| none | 0 | 3.1 / 5.9 ms | 200 | 1.8 / 3.2 / 3.2 s | 3.0 s | 35 / 76 / 77 ms | 57 ms |
| small | 64 MiB | 98.6 / 126.6 ms | 200 | 10.9 / 21.3 / 22.1 s | 22.0 s | 44 / 74 / 74 ms | 57 ms |
| small | 256 MiB | 474 / 515 ms | 102-124 | 49.9 / 51.3 / 51.3 s | 51.0 s | 32 / 68 / 70 ms | 56 ms |
| small | 960 MiB | 1,684 / 1,752 ms | 31 | 52.5 / 53.2 / 53.3 s | 53.1 s | 33 / 59 / 63 ms | 56 ms |
| large | 64 MiB | 18.3 / 24.3 ms | 200 | 3.3 / 6.0 / 6.2 s | 5.9 s | 32 / 64 / 66 ms | 57 ms |
| large | 256 MiB | 49.0 / 74.0 ms | 200 | 7.0 / 12.5 / 13.0 s | 12.8 s | 25 / 61 / 63 ms | 56 ms |
| large | 960 MiB | 162.8 / 242.0 ms | 200 | 18.2 / 35.3 / 36.9 s | 36.9 s | 27 / 57 / 63 ms | 56 ms |

The 256 MiB small-padding point is the representative case for the
headline comparison.

**DeltaForge's activation latency by activation-order decile** (p50, s):

| Padding, offset | 1st | 2nd | 3rd | 4th | 5th | 6th | 7th | 8th | 9th | 10th |
|---|---|---|---|---|---|---|---|---|---|---|
| none, 0 | 0.37 | 0.52 | 0.95 | 1.34 | 1.62 | 1.99 | 2.31 | 2.62 | 3.00 | 3.19 |
| small, 64 MiB | 1.6 | 4.8 | 3.4 | 7.9 | 9.9 | 12.1 | 14.2 | 16.5 | 19.0 | 21.3 |
| small, 256 MiB | 7.7 | 14.6 | 17.8 | 34.4 | 42.7 | 50.5 | 50.7 | 50.9 | 51.1 | 51.3 |

Debezium's deciles stay between 6 and 65 ms at every offset.

DeltaForge's seek per proof is flat across the deciles at each offset (for
example 89-112 ms at 64 MiB small). The growth by order is queueing: proofs
are serial, and each table waits for every proof before it. Even on a fresh
file, with only about 0.7 s of seek in all, the first row of the last tenth
of tables waits 3.2 s.

## Projection to the 1 GiB default

The seek per proof is linear in the offset (least squares, R² > 0.999):

- **small padding:** 1.76 ms per MiB, so about **1.8 s per proof at
  1 GiB** (`max_binlog_size`, the default);
- **large padding:** 0.16 ms per MiB, about 0.17 s at 1 GiB.

The projection is for an OLTP file (small padding) on this host. There, 200
newly active tables need about 360 s of serial seek if each proof scans. In
practice the queue then grows faster than it drains. Requests pile up and
share later scans (only 31 physical scans for 200 proofs at 960 MiB), and
every row of the pipeline waits. At 256 MiB and above, DeltaForge's
activation and steady latencies were bounded only by the 50 s run.

Debezium's first-row latency did not depend on the offset (25-44 ms p50).

## What this pins

- **Cost model:** proof cost is set by the events in the binary log file
  before the proof, not by the interval it proves.
- **Serialization:** proofs are serial even on a fresh file, so activation
  latency grows linearly with activation order.
- **Coalescing:** it happens only once the pipeline has already fallen
  behind.

All three are inputs to the release-blocking fix
(`docs/design/mysql-proof-seek.md`).

## Superseded runs

`target/perf/sensitivity` and `sensitivity-small` (harness `48987eee` and
`528f321f`) ran the same points with the catch-up phase. Where DeltaForge
fell far behind, the catch-up window covered nearly every row. Activation
latency there rested on 17 to 165 of the 200 first rows. Both are kept,
each with a `SUPERSEDED.txt`, and not used here.
