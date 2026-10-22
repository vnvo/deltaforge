# MySQL proof scans: the seek from the start of the binary log file

Status: open, release-blocking. It blocks dense active-set scaling, T1a and
rc.1. This document records the defect and the review's requirements. The
fix is not designed yet.
Base: main `2613e9b1`.

## Defect

DeltaForge proves each newly active table before its first event: a proof
scans the binary-log interval (A,B] between the source's checkpoint and the
table's capture (`mysql_binlog_scan.rs`). In GTID mode the scan opens a
fresh replication stream positioned by the GTID set A. MySQL has no index
from a GTID set to a file offset. To find the first transaction not in A,
the server reads the binary log file from its start and skips every
executed transaction.

The cost of a proof therefore grows with the bytes of the file before A, not
with the interval it proves. Proofs run one at a time, so rows of other
tables wait behind them.

## Evidence

The study's per-proof trace (`deltaforge::proof_trace`): 1,000 tables, 20%
active, 2,000 single-row inserts a second. The same DeltaForge build ran
twice in one invocation. Each run performed 200 proofs, one per newly
active table, all `proven`.

| | Proofs about 1.5 MB into the file | Proofs about 35 MB into it |
|---|---|---|
| Seek per proof, p50 / p95 | 4.5 / 10.6 ms | 53 / 74 ms |
| Seek, total | 1.0 s | 11.0 s |
| Time spanned by the 200 proofs | 4.5 s | 13.7 s |
| Interval scanned per proof, p50 | 188 events, 12 KB | 668 events, 41 KB |
| Capture: connect / lock wait / schema, p50 | 1.4 / 0.5 / 6.5 ms | 1.2 / 0.5 / 5.6 ms |
| Steady-state row latency, p99 | 3.8 s | 12.9 s |

- The seek grows by about 1.5 ms per MB of file before the proof. The scan
  of the interval itself stays at 0.5-1.6 ms. Capture, lock wait and
  connection time stay small and flat.
- The proofs' total time matches their span: they are serial, and the
  pipeline waits on them.
- MySQL's default `max_binlog_size` is 1 GiB. On the measurement host a
  proof near the end of a file would cost about 1.5 s, so a burst of
  thousands of newly active tables would wait for hours.
- Debezium, which never reopens its stream, delivered the first row of a
  table in 30-150 ms in the same runs.

**Controlled offsets.** In the same point, the binary log was flushed and
then padded to a fixed starting offset (one repetition each):

| Starting offset | Seek per proof, p50 | Seek, total | Activation p50 / p99 | Steady p99 |
|---|---|---|---|---|
| 0 (fresh file) | 2.7 ms | 0.58 s | 1.5 s / 2.9 s | 2.7 s |
| 960 MiB | 190 ms | 32.2 s | 15.2 s / 29.5 s | 29.4 s |

**The seek follows events, not bytes.** The offsets above were padded with
8 MiB rows. Padded instead with single-row transactions like an OLTP
server's (3 repetitions), the seek per proof at 64 MiB was 96 ms p50 (124
ms p95), against 17 ms with large rows. The server skips event by event, so
a file of small transactions costs several times more per byte. The
sensitivity series (`crates/perf-harness/results/scale-study-design.md`)
measures both paddings. A projection to the 1 GiB default uses the
small-transaction curve.

**Requests already coalesce, but only under pressure.** A proof can be
served from the retained record of an earlier scan
(`"served":"record"` in the trace), instead of scanning. With
single-row-transaction padding, the physical scans out of 200 proofs were:

- 200 at 64 MiB;
- 76 to 84 at 256 MiB;
- 23 at 960 MiB.

So the slower each seek, the more requests queue behind it and share the
next scan. A fix must bound physical scans by design, not only once the
pipeline has fallen behind.

**Serial even without the seek.** On a fresh file, activation latency still
grows linearly with activation order, by decile of the 200 activations:
205, 582, 868, 1,064, 1,350, 1,627, 1,948, 2,226, 2,527 and 2,840 ms. The
total seek is only 0.58 s there. Each proof's other costs are small, about
13 ms with connect, schema capture, setup and seek, but they run one after
another. Positioned scanning alone therefore does not meet the regression
"activation latency does not grow linearly with activation order":
coalescing or session reuse is needed as well. Debezium's deciles stay flat
(28-182 ms).

Raw traces: the study's `act-diag` run on the measurement host
(`runs/*/state/stderr.out`).

## Fixes to examine

Both are complementary.

1. **Positioned GTID scanning.** Start the GTID replication stream from the
   known binary log file and transaction-boundary offset of A, while GTID
   membership remains the authoritative (A,B] rule.
2. **Request coalescing and session reuse.** Concurrent first-touch proofs
   share one extension to the greatest required frontier, instead of each
   reopening the stream and seeking on its own.

## Correctness conditions

- The file and offset belong to the same server UUID and lineage.
- The offset is a proven event and transaction boundary.
- GTID membership remains authoritative.
- Rotations and purged files fail closed.
- A server that reaches a different terminal position is refused.
- Cancellation, or a failed positioned scan, leaves the scanner's state
  unchanged.
- Shared and direct scans remain statement- and digest-identical.
- No silent fallback to an unbounded scan from the file's start.
- Reconnect, failover and restart behavior remains correct.

## Required regression

- A fixed interval and statement set at offsets of 0, 64 MiB, 256 MiB and
  near 1 GiB.
- Proof work approximately proportional to the requested interval, not to
  the bytes of the file before it.
- A burst of hundreds of newly active tables performs a bounded number of
  physical seeks or extensions.
- Activation latency does not grow linearly with activation order.

## Related

- Pinned before this fix by the scaling study's sensitivity series
  (`crates/perf-harness/results/scale-study-design.md`): first-touch latency
  at binary-log offsets of about 0, 64 MiB, 256 MiB and 1 GiB.
- Tracked separately, at lower priority: the operator step a crash needs
  (`store-gate break`), and the latency spike after a DDL
  (`crates/perf-harness/results/2026-10-debezium-comparison.md`).
