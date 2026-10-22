# MySQL proof scans: the seek from the start of the binary log file

Status: open, release-blocking. It blocks dense active-set scaling, T1a and
rc.1. Design revision 1, for review: no production code until the
safe-anchor and coalescing designs are approved. Protocol probes are part of
validating it (section "Probes").
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

## Design, revision 1

### How proofs run today (facts the design rests on)

Code references are to main `2613e9b1`, `crates/sources/src/mysql/`:

- **Serial on the stream.** The source is one task. A row of an unproven
  table is proved before it is decoded, inline:
  `dispatch_event` → `select_for_rows` → `establish_at` (`mod.rs:1855`,
  `mysql_event.rs:329`, `mysql_selection.rs:658`,
  `mysql_baseline.rs:355`). The main binary-log stream stops while a proof
  runs, and two requests are never outstanding at once.
- **Per-proof work.** Each lazy proof has two parts:
  - a capture: a new connection pool, a read-only transaction, the table's
    metadata lock, `SHOW BINARY LOG STATUS` before and after the schema
    read (`mysql_binlog_scan.rs:976-1069`);
  - an interval request (A,B] on the shared scanner.
- **A, B and the boundary.** A is the stream's last transaction boundary,
  `txn_eval_cp`. It pairs `last_file/last_pos` (the end of the commit
  event) with `last_gtid`, both from the stream (`mod.rs:2449`). B is the
  capture's second status read.
- **Scanning.** A scan is GTID-positioned in GTID mode:
  - `client.gtid_set = from.gtid_set`, and `from.file/pos` are ignored
    (`mysql_binlog_scan.rs:634`);
  - the vendored dump command sends an empty file name and position 4
    (`dump_binlog_gtid_command.rs:24-32`). MySQL picks the file from the
    set and reads it from its start: the defect.
  - A scan is reached when `B ⊆ A ∪ seen`, where `seen` is the GTIDs it
    read. A GTID outside B is an overshoot, and a transaction without a
    GTID is a framing error (`complete_txn`, `mysql_binlog_scan.rs:352`).
- **Identities.** Digests and persisted identities use GTID sets only, not
  offsets (`scan_digest`; `mysql_checkpoint_position` in GTID mode;
  `mysql_activation.rs:283-360`).

### The safety argument: the offset is a hint, GTID accounting is the proof

The scan's completion rule already decides correctness without the physical
start:

- **Too early.** A start before boundary A only reads transactions already
  in A. They are skipped by membership, so the cost is higher and the
  result unchanged.
- **Too late.** A start after boundary A never reads some transaction g in
  B \ A. g is never added to `seen`, so `B ⊆ A ∪ seen` cannot hold and the
  scan cannot reach B. It fails closed. It can never "reach B while
  omitting a DDL".
- **Exactly at A.** This is the case the anchor aims for: the cost is
  proportional to the interval.

**Preconditions.**

- **P1. A and B are exact GTID sets:** no GTID claimed that the server has
  not executed. Today this does not always hold. `merge_gtid`
  (`mysql_event.rs:694-733`) rewrites `uuid:1-5:7-9` plus `uuid:10` as
  `uuid:1-10`, so a set with gaps over-claims. An over-claimed g located
  after A's offset would be skipped as "already in A". That hides a DDL
  today, independently of this fix. P1 therefore requires replacing
  `merge_gtid` with exact set insertion, and a probe confirms the gap case
  is reachable (probe 6). The issue is raised as its own correctness
  finding.
- **P2. Every transaction in the interval carries a GTID.** This is already
  enforced: anonymous GTIDs fail with `Framing`.
- **P3. The physical start is a transaction boundary in a file of the same
  lineage.** A start inside a transaction must never splice a partial
  transaction into `seen`. This is validated on every open (below).

Under P1-P3, any anchor is safe; a wrong one only costs time or fails
closed. The design then needs only to make the right anchor the common case,
and to fail fast rather than slow when it is wrong.

### Where each physical coordinate comes from

Anchors are taken only from the source's own binary-log stream, never from
`SHOW BINARY LOG STATUS` or another separately read status.

- **Lazy, full-fallback and forward proofs (boundary A = `txn_eval_cp`).**
  The anchor is the start offset of the first transaction after A. When the
  stream reads that transaction's GTID event, it knows the event's
  `next_event_position` and `event_length`. Their difference is the
  transaction's start: the exact end of A, in the current file.
  - The stream records an `Anchor {file, offset, gtid_of_next_txn,
    set_before = A, server_uuid, lineage}` at each GTID event.
  - The transaction whose rows trigger the proof is exactly the first one
    after A, so the anchor exists whenever a row-triggered proof does.
- **After a restart or reconnect.** The stream resumes by GTID set, and the
  server skips in-set transactions before sending anything. The first
  transaction the stream receives is therefore the first one not in the
  resume set. Its GTID event gives the anchor above, with `set_before` = the
  resume set. No separately read position is used.
  - Before that first transaction arrives, no row-triggered proof can
    happen.
  - Today `last_file/last_pos` are stale or empty until the first commit
    after a resume (`mod.rs:1684`, `helpers.rs:57-91`). Under this design
    they are never used as an anchor.
- **Snapshot baselines (A = the stream's start) and failover drift (A = F).**
  These have no stream-observed anchor when they start. They use the
  bounded alternative below.

### Why the anchor is not after A

The anchor of A is the start of the first transaction the stream received
after the boundary it evaluated at A. Two cases:

- **Within one stream connection,** the server sends transactions in
  binary-log order without gaps, apart from in-set transactions skipped on
  a GTID-positioned connect. So every transaction located before the anchor
  was either received earlier (and is in A) or skipped as in-set (and is in
  A).
- **Across a rotation,** the anchor's file is the file where that GTID
  event was read (the stream's Rotate events keep `last_file` current).

### Validation after opening the stream at an anchor

The scan opens a file/position dump (`COM_BINLOG_DUMP` with file and
offset, or `COM_BINLOG_DUMP_GTID` with file, offset and set, if probe 1
shows the server honors both). Then, before any statement counts:

1. **Identity.** The session's `server_uuid` must be the lineage's
   (existing check, `mysql_session.rs:137`), and the anchor's lineage must
   match the request's.
2. **Boundary.** The first event after the server's protocol events
   (Rotate, Format_description) must be a GTID event that starts exactly at
   the anchor offset and carries `gtid_of_next_txn`. Anything else is
   refused: a Rotate to another file, another GTID, a non-GTID event, or a
   different offset. This proves the offset is a transaction boundary of
   the same file and history, and that the file was not rotated away or
   rewritten.
3. **Accounting.** The usual rules apply from there: membership against A,
   overshoot against B, reached when `B ⊆ A ∪ seen`.
4. **Fail fast.** Today a scan that cannot reach B waits for the 600 s time
   limit. A positioned scan instead stops with `NotReached` when it reaches
   the server's live end without completing. That is detected by a
   heartbeat after the last event, with nothing left to send. A late or
   wrong anchor therefore costs one short scan, not ten minutes.

### Stale, purged, rotated, inconsistent or foreign hints

| Hint | What happens |
|---|---|
| File purged | The server refuses the dump. The request falls to the bounded alternative, and a metric and trace record it. |
| File rotated since (still present) | The scan starts in the older file and crosses the rotation. This is valid: the boundary check passes. |
| Boundary check fails (other GTID, offset not a boundary, rewritten file) | Refused with `AnchorMismatch` before any state changes, then the bounded alternative. Counted and traced. |
| Another server or lineage | Refused by the identity check (existing `OtherServer`), with no fallback: the request fails closed as today. |
| `NotReached` from a positioned scan | Refused with no partial state, then the bounded alternative once. A second failure fails the request closed. |

### The bounded alternative when no safe anchor exists

There is never a silent scan of an arbitrarily large file prefix:

- The alternative is today's GTID-positioned scan, run under an explicit
  cost budget. A new limit, `max_seek`, bounds the time to the first
  transaction (default: a few seconds, configurable). When it is exceeded
  the request fails closed with a typed `SeekLimit` error and an incident.
  It never falls back further.
- Every alternative scan is counted
  (`deltaforge_mysql_proof_unanchored_total{kind}`) and traced. The scan
  that serves a snapshot baseline or failover drift also yields an anchor
  for every later request: the first transaction it receives gives one, as
  for the stream. So the unanchored cost is paid at most once per snapshot
  or failover, not per table.

### Proof identities and digests stay GTID-only and byte-identical

- **Inputs unchanged.** The digest inputs stay `(A, B)` as GTID-set
  `WmPos`, the classified statements, and the classifier version.
  Statements are identified by GTID and ordinal. A positioned scan reads
  the same transactions of (A,B] as a GTID-positioned one, because both
  apply the same membership rule. Starting earlier only adds skipped
  in-set transactions.
- **Offsets excluded.** Anchors and offsets never enter a digest or a
  persisted identity.
- **Test.** For every interval of the regression matrix, the positioned and
  GTID-positioned scans must give identical statements and identical
  digests, by direct comparison.

### Coalescing: from serial proofs to bounded batches

Two layers are needed, because proofs never overlap today.

**Layer 1: an activation window in the source.** When the stream meets a
row of an unproven table, it does not prove that table at once:

- It reads ahead, holding raw events in order in a bounded buffer: at most
  `activation_window_ms` (default 50 ms), a maximum number of events and a
  byte budget. While reading ahead it collects every other unproven table
  first touched in the window.
- It then proves the whole set at once:
  - **one capture transaction.** It takes the metadata locks of all the
    tables, reads the position, reads every schema in one batched catalog
    query, and reads the position again. The per-table capture semantics
    hold, because each table's lock is held across both reads.
  - **one scanner request** (A_head, B], where A_head is the boundary
    before the window's first first-touch.
- Each table is answered over its own (A_i, B] from that one record: A_i is
  the boundary before its first transaction. A_i ≥ A_head, so no further
  scan is needed. Each answer has its own membership and its own digest.
- Then the buffered events are dispatched in order. Output order does not
  change: nothing is emitted until its table is proved, as today.
- If the window's capture or scan fails, no table of the window is marked
  proved, and the buffered events are dispatched only after a successful
  retry, or the source fails closed as today.

**Layer 2: a coalescing scanner (state machine).** Layer 1 produces bursts.
The scanner must also handle concurrent requests (snapshot pages, failover
drift, future parallel sources) without one physical scan per request:

- **Pinning.** A request pins its own A and B before it waits (existing
  pins). Pruning never drops statements a pinned or running request needs.
- **Rounds.** At most one physical extension is in flight. When it
  completes, the next round's target is the greatest B of the requests
  queued *before that round started*. Requests arriving during a round wait
  for the next. Each request is therefore served within two rounds, so a
  continuously advancing frontier cannot starve an earlier request.
- **Shared work.** One extension serves every waiter of its round. A
  cancelled waiter unpins itself only. The round continues while any other
  waiter needs it, and is abandoned (its connection reset) only when none
  does.
- **No partial commit.** An extension is built into a staging record and
  committed to the shared record only when it completes. Today's left
  extension mutates the record before the right one runs
  (`mysql_proof_scanner.rs:275-280`); that is changed. A failed or
  cancelled round leaves the record exactly as it was.
- **Answers.** Each request is answered from the committed record over its
  own (A,B], with its own digest (as `answer` does today).
- **Bound.** Per round, at most two physical scans: one left extension and
  one right extension.

**Bounds for a burst.** A simultaneous burst of N first touches inside one
activation window costs:

- one capture transaction;
- at most two physical scans, or one when the record already reaches A.

Across a burst longer than a window, the number of windows (and so of
scans) is at most ceil(burst duration / window). The design target is
explicit: 200 first touches within one window give at most 2 physical scans
and 1 capture.

### Acceptance targets

The review's targets, each with a measured check:

| Target | Check |
|---|---|
| Seek work proportional to the interval, not the preceding file | Seek per proof flat (within 2x) at offsets 0, 64, 256 and 960 MiB, small-transaction padding (the sensitivity series) |
| Bounded physical scans for a burst | 200 first touches in one window: at most 2 scans and 1 capture (trace counts) |
| Activation latency flat by order | Activation-order decile p50s within 2x of each other, no linear trend, at 0 and 256 MiB |
| The 256 MiB case meets the budget | 1,000 tables, 20% active, 256 MiB small padding: activation p99 under 5 s (now about 51 s) |
| Identity unchanged | Positioned, GTID-positioned and shared answers statement- and digest-identical over the regression matrix |
| Existing regressions green | Reconnect, failover drift, rotation, purge, cancellation, scan limits, dual coordinate, shared-vs-direct, proof matrix |
| Mutants caught | Disabling the positioned start, or the activation window, fails the performance regression |

### Probes

Run on throwaway MySQL 8.4 containers, before any production code:

1. **Positioned GTID dump.** Does `COM_BINLOG_DUMP_GTID` honor a file
   name and offset together with a GTID set? Where does it start, and what
   does it cost at 960 MiB?
2. **Position dump with client membership.** `COM_BINLOG_DUMP` at a
   boundary offset, with GTID membership applied by the client: the cost at
   960 MiB, and whether the events received are identical to the GTID dump.
3. **Bad offsets.** An offset inside an event, inside a transaction, and
   beyond the file's end: the server's behavior (error or garbage) and the
   boundary check's response.
4. **A too-late anchor.** An anchor after A with a DDL in between. The scan
   must fail closed (`NotReached`) quickly, never reach B.
5. **Purged and rotated anchor files.** The error from a purged file, and a
   scan crossing a rotation from an older anchor.
6. **GTID gaps.** Can a source's executed set have gaps that later fill?
   Examples: transactions with explicit `gtid_next`, or a multi-threaded
   replica without commit-order preservation. Does `merge_gtid` then
   over-claim in a way that loses a transaction on resume, or hides a DDL
   from a proof?
7. **Batched capture.** One transaction locking N tables' metadata, with
   one batched schema query: its cost for N = 1, 10, 200, and its behavior
   with a concurrent DDL on one of them (it must wait, or time out
   cleanly).
