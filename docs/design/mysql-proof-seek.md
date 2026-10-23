# MySQL proof scans: the seek from the start of the binary log file

Status: open, release-blocking. It blocks dense active-set scaling, T1a and
rc.1. Design revision 2 (positioned scanning final for review; coalescing
being finalized): no production code until the safe-anchor and coalescing
designs are approved. Protocol probes are part of
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

### Positioned GTID scanning, revision 2 (final for review)

Revision 2 replaces revision 1's anchor, validation and fallback sections,
using the probe results. Precondition P1 (exact GTID sets) holds since #170.

**The protocol.** Probe 1 established that `COM_BINLOG_DUMP_GTID` honors a
file name and offset sent together with the GTID set. The server starts at
the offset and still skips in-set transactions. The vendored dump command
(`dump_binlog_gtid_command.rs`) gains the file name and offset; today it
sends an empty name and offset 4. This is a vendored patch, recorded like
the others. With an empty file name the command behaves exactly as today,
which is the bounded alternative's path.

**An anchor** is
`Anchor { server_uuid, lineage, file, offset, set_before: GtidSet, next_gtid: Option<String> }`.
It means: in `file` of that server, the transaction starting at `offset`
is the first one not in `set_before`, and `next_gtid` is its GTID when
known. It is held in memory only and never persisted; after a restart
anchors are observed again.

#### 1. Where each physical coordinate comes from

Every anchor is observed in a binary-log stream at a transaction boundary.
None is taken from `SHOW BINARY LOG STATUS`, `@@gtid_executed` or any other
separately read status: those reads are not atomic with their GTID set (the
code says so, `mysql_proof_scanner.rs:966`). The capture positions of B
therefore never become anchors.

| Anchor | Observed where | `offset` | `set_before` | `next_gtid` |
|---|---|---|---|---|
| **Stream anchor** (lazy, full-fallback and forward proofs; A = `txn_eval_cp`) | the main stream, at the GTID event of the transaction after A, the one whose rows trigger the proof | that event's start: `next_event_position - event_length` | A, the executed set at that boundary | that event's GTID |
| **Record frontier** (the shared record's right end, to extend it) | the scan that reached the record's `hi`, at its terminal event | the terminal event's end offset, the boundary after the last transaction of `hi` (`ScanTrace::terminal`) | `hi`'s GTID set; at reach `done == hi` exactly, because a GTID outside `hi` is an overshoot | not known (the next transaction was not read) |
| **Session anchor** (the first scan of a GTID-positioned stream: the bounded alternative's yield) | that scan, at the first transaction the server sends | that GTID event's start | the set the stream was opened from | that GTID |

A compressed transaction is one payload event preceded by its own GTID
event. Its GTID event start and payload end are boundaries, as for any
transaction (probe 8 below confirms the GTID event is outside the payload).

#### 2. Why the anchor is not after boundary A

- **Stream anchor.** The main stream received every transaction between
  its connect and this one, in binary-log order. A GTID-positioned connect
  skips only transactions in its start set. So every transaction located
  before the anchor offset was either received earlier, and so is in A, or
  skipped as in-set, and so is in A. The transaction at the anchor is the
  first one after A.
- **Record frontier.** The scan reached `hi` at its terminal event. Every
  transaction before the terminal's end was read and added to `done`, or
  skipped as in its start set. So `done == hi` covers everything before the
  offset, and the next transaction is the first one not in `hi`.
- **Session anchor.** The same argument as for the stream anchor, applied to
  the scan's own stream.

In every case the claim rests on reading the stream, never on a status
read. An anchor whose claim does not hold is caught by validation, or at
worst by GTID accounting (section 4).

#### 3. Validation after opening a stream at an anchor

The scan opens `COM_BINLOG_DUMP_GTID` with the anchor's file and offset and
the request's GTID set A. A uses the anchor's `set_before` when the anchor
was taken at A; for a frontier anchor, A is the record's `hi`. Before any
statement counts:

1. **Identity.** The session's `server_uuid` must be the request's (the
   existing check, on the session that streams), and the anchor's
   `server_uuid` and `lineage` must match the request's. An anchor of
   another server or lineage is never used, not even as a hint.
2. **Boundary.** After the server's protocol events (Rotate to the anchor
   file at the anchor offset, Format_description), the first event must be
   a GTID event that starts exactly at the anchor offset, in the anchor's
   file. When `next_gtid` is known it must be that GTID. When it is not
   known (a frontier), the GTID must not be in the anchor's `set_before`.
   Anything else is refused with `AnchorMismatch` before any state changes:
   - a `BEGIN` without a GTID (probe 3: an offset inside a transaction);
   - a Rotate to another file;
   - another GTID, or an in-set one;
   - a different start offset.
3. **Accounting.** The scan then runs under today's rules, unchanged:
   membership against A, overshoot against B, and reached only when
   `B ⊆ A ∪ seen`.
4. **Fail fast.** A positioned scan that reaches the server's live end (a
   heartbeat with nothing more to send, after the last event) without
   reaching B stops with `NotReached`. B was captured before the scan
   started, so all of B is already in the binary log, and reaching the end
   without it means the start was wrong. This turns probe 4's silent late
   start into a prompt refusal, where today it would wait out the 600 s
   time limit.

#### 4. Stale, purged, rotated, inconsistent or foreign hints

| Hint | Outcome |
|---|---|
| The anchor's file was purged | The server refuses the dump (probe 5). Falls to the bounded alternative once; counted and traced. |
| The file rotated since (still present) | Valid. The scan starts in the older file and crosses the rotation (probe 5). The boundary check passes. |
| The offset is inside an event | The server refuses ("truncated in the middle of event", probe 3). Bounded alternative once. |
| The offset is inside a transaction | The first event is a `BEGIN` without a GTID (probe 3). `AnchorMismatch`, then the bounded alternative once. |
| The offset is a boundary but after A (stale or wrong) | The boundary check refuses it when `next_gtid` is known. Otherwise GTID accounting cannot reach B (probe 4: 4 GTIDs missing) and the scan stops at the live end (`NotReached`). Bounded alternative once. Never a proof that omits a DDL. |
| Another server or lineage | Refused by the identity checks, with no fallback. The request fails closed as today. |
| The bounded alternative fails too | The request fails closed, as today. |

A positioned scan that fails changes no scanner state (section 6).

#### 5. How a restart obtains a safe anchor

Anchors are not persisted. After a restart or reconnect the main stream
opens GTID-positioned from its exact resume set, as today; that seek is
paid once per connect, not per proof. The first transaction it receives
gives a stream anchor (section 1): the first transaction not in the resume
set, at its observed offset. No row-triggered proof can happen before that
transaction arrives, so every lazy, full-fallback and forward proof after a
restart has a stream anchor.

Proofs with A at the stream's start (snapshot baselines) or at a failover
anchor F have no stream anchor. They use the bounded alternative once,
which yields a session anchor for every later request at that boundary.

#### 6. No partial scanner state

A positioned scan is built into the request's own staging (see the
coalescing state machine). The shared record, its frontier anchor and the
anchor cache are updated only when the scan reaches B and its accounting
holds. A refused anchor, a server error, cancellation or a fallback leaves
them exactly as they were.

#### 7. Proof identities and digests stay GTID-only and byte-identical

- **Digest inputs unchanged:** the domain, the classifier version, A and B
  as GTID-set `WmPos`, and the classified statements identified by GTID and
  ordinal.
- **The start is not part of the proof.** A positioned scan and a
  GTID-positioned one read the same transactions of (A,B] under the same
  membership rule. A start earlier than A only adds skipped in-set
  transactions; a start later than A is refused.
- **Excluded from every digest and persisted identity:** anchors, offsets
  and the start mode (positioned or not).
- **Regression:** for every interval of the matrix, positioned, unpositioned
  and shared answers are statement- and digest-identical.

#### 8. The bounded alternative (no safe anchor)

There is never a silent scan of an arbitrarily large file prefix:

- **When it runs:** only when no anchor exists (a snapshot baseline,
  failover drift), or after one refused anchor.
- **Its form:** today's GTID-positioned scan under a new limit, `max_seek`,
  the time from opening to the first transaction (default 10 s,
  configurable). When it is exceeded the request fails closed with a typed
  `SeekLimit` error, and the pipeline stops with an incident whose evidence
  includes the seek time and the file. A prefix too large to seek within
  the bound is a visible stop, never a silent cost.
- **Observability:** every run is counted
  (`deltaforge_mysql_proof_unanchored_total{kind,reason}`) and traced.
- **Its yield:** each run yields a session anchor, so the next request at
  the same boundary is positioned. The cost is paid at most once per
  snapshot, failover or refused anchor, never per table.

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

### Probe results (probes 1-6)

MySQL 8.4 in a throwaway container. The evidence is in
`docs/design/evidence/mysql-proof-seek/`: the logs, the driver scripts, and
the probe (`seekprobe.rs`, a copy of the vendored client whose GTID dump also
sends a file name and offset). Boundary A is taken while the server is quiet.
The interval (A,B] then holds 7 transactions, the 4th a DDL.

At 964 MiB into a file of 3.35 million single-row transactions:

| Probe | Time to the first transaction | Every GTID of B \ A read | Outcome |
|---|---|---|---|
| 1. GTID dump, as today (no file name, offset 4) | 1,252 ms | yes | correct |
| 1. GTID dump with the file name and A's offset | 2.0 ms | yes | correct, statements identical |
| 2. File/position dump at A's offset | 1.8 ms | yes | correct |
| 3. Offset inside an event | refused: "binlog truncated in the middle of event" | - | fails closed |
| 3. Offset just after the GTID event (inside a transaction) | starts at `BEGIN`, no GTID | no (1 missing) | fails closed: `Framing`, and the accounting |
| 3. Offset past the file's end | refused | - | fails closed |
| 4. Anchor after the DDL transaction | 1.4 ms, starts at a later GTID | **no (4 missing, the DDL among them)** | the server accepts it silently; only `B ⊆ A ∪ seen` refuses it |
| 5. Anchor in an older file, across a rotation | 1.8 ms | yes | correct |
| 5. Anchor in a purged file | refused: "Could not find first log file name" | - | fails closed |

Findings:

- **The protocol supports positioned GTID scanning.**
  `COM_BINLOG_DUMP_GTID` honors a file name and offset together with the
  GTID set. The server starts at the offset and still skips in-set
  transactions. The vendored client never sends them. This one change
  removes the prefix cost: 1,252 ms becomes 2.0 ms at 964 MiB.
- **The server validates framing, not history.** It refuses an offset
  inside an event, past the end, or in a purged file. It accepts any event
  boundary, including one inside a transaction or after A.
- **A late anchor is silent at the server.** The client's GTID accounting
  is the only check that refuses it, which is why the design makes it the
  authority. The boundary check (the first event must be the expected GTID
  event at the anchor offset) refuses the inside-transaction case before
  any statement counts.

### Probe results (probes 7 and 8)

MySQL 8.4 in throwaway containers; evidence in
`evidence/mysql-proof-seek/batched-capture-probe7.log` and
`frontier-probe8.log`, with the scripts.

**Probe 7, batched capture.** One transaction takes N tables' metadata locks
(`SELECT 1 FROM t LIMIT 0`), then the position, then one schema query for
the N tables (the loader's own query, which already takes a list of
tables), then the position again. Compared with N captures of one table
each, each on its own connection as today. Times are wall clock, including
about 85 ms of `docker exec` per run:

| N | N single captures | One batched capture |
|---|---|---|
| 1 | 88-101 ms | 88-89 ms |
| 10 | 91-118 ms | 83-102 ms |
| 200 | 576-661 ms | 106-138 ms |

The batch's schema query returned every column of the 200 tables (800
rows). Under concurrency:

- **A DDL on a table the batch has locked waits** until the batch commits
  (2.1 s, against 2 s held). The capture semantics hold: no DDL lands
  between a table's lock and the second position read.
- **A table locked by another session**, with `lock_wait_timeout = 1`: the
  batch fails with 1205 and holds no lock afterwards
  (`performance_schema.metadata_locks` is empty for it).
- **Lock order matters.** Against a two-table `RENAME TABLE t010 TO ..., t011
  TO ...`, a batch locking t010 then t011 (sorted) never deadlocks. One
  locking t011 then t010 deadlocks, and MySQL aborts the batch with 1213.
  A batched capture must therefore lock in sorted `(schema, table)` order,
  the order MySQL takes metadata locks for multi-table statements, and
  treat 1213 as retryable.

**Probe 8, revision 2's assumptions.**

- **(a) Compressed transactions.** A positioned start at a compressed
  transaction's offset gives Rotate, Format_description, then the
  transaction's GTID event at exactly the offset, followed by the payload.
  The GTID event is outside the payload, so the boundary check applies
  unchanged.
- **(b) Record frontier.** Opened at B's end offset with set B, the stream
  sends heartbeats until the next transaction is committed. Then the first
  event is that transaction's GTID event (`:9`, not in B), starting
  exactly at the frontier offset. The frontier check (the first GTID must
  not be in `set_before`) holds.
- **(c) The live end.** With nothing new, the server sends only heartbeats,
  each carrying the current end position. A heartbeat at the stream's own
  position, with B not reached, is the live end. Fail-fast uses exactly
  this.

### Finding: an over-claimed GTID set loses rows across a restart

This is probe 6. It is a correctness defect of its own, independent of the
seek, and confirmed end to end with the DeltaForge runner of main
(`cd716061`), an HTTP sink and SQLite state
(`evidence/mysql-proof-seek/gtid-gap-restart.log`):

1. The server has executed `u:1-8`. Row 1 is delivered.
2. Row 2 is committed with an explicit `gtid_next = u:13`, so the server's
   set is `u:1-8:13`. Row 2 is delivered. **DeltaForge's stored checkpoint
   is `u:1-13`.**
3. DeltaForge stops. Rows 3 to 6 are committed. Automatic GTIDs take the
   lowest free numbers, `u:9-12`.
4. DeltaForge restarts and resumes from `u:1-13`. The server skips 9-12 as
   already executed. **Rows 3 to 6 are never delivered.** No error,
   incident or gap is reported.
5. Row 7 (`u:14`) is delivered, and the checkpoint moves on.

The cause is `merge_gtid` (`mysql_event.rs:694-733`). It keeps only the
start of the first interval of the transaction's UUID and its new number,
so any gap in the executed set collapses.

**When this happens in production:**

- Explicit `gtid_next` is the reproducible trigger here.
- Executed sets with gaps that fill later also arise under MySQL Group
  Replication and InnoDB Cluster. Each member assigns GTIDs from its own
  block (`group_replication_gtid_assignment_block_size`, default 1,000,000).
  This is from MySQL's documented behavior, not probed here.
- A source with such gaps can lose rows across any restart or reconnect.
  The same over-claim also breaks precondition P1 of this design: a proof
  can treat a later transaction as "before A" and skip its DDL.

**Confirmed on Group Replication** (`evidence/mysql-proof-seek/group-replication-*.log`): a two-member multi-primary group with a GTID block size of 100, rows on both members (executed set `group:1-3:101`), a stop, four rows on member 1, a restart, one more row. The main build stored `group:1-101` and lost rows 3 to 7. The fixed build stored `group:1-3:101` and delivered all seven once.

**Fixed** in #170 (`cae39fa1`): one exact `GtidSet`, and old GTID state refused.

**Fix direction:** keep exact GTID sets.

- Insert each transaction's GTID into a parsed interval set (the scanner
  already has exact set arithmetic, `parse_gtid_set`).
- Persist the exact set.
- Never extend an interval over numbers not seen.
- Regression: the steps above must deliver rows 3 to 6. It also needs a
  proof test where a DDL fills a gap after A.
