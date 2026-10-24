# MySQL proof scans: the seek from the start of the binary log file

Status: open, release-blocking. It blocks dense active-set scaling, T1a and
rc.1. Design revision 3, final for review (positioned scanning and
coalescing). Revision 3 corrects revision 2's review findings: frontier
anchors across a rotation, a real capture bound, exact physical-work bounds
including failures and fallback, and all-or-nothing batch outcomes. No
production code until both designs are approved. Protocol probes are part of
validating it (section "Probes").
Base: main `cae39fa1` (exact GTID sets, #170).

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
2. **Boundary: a strictly validated chain to the first GTID.** The
   server opens the stream with an artificial Rotate, which must name the
   anchor's file and offset, and an artificial Format_description.
   Artificial events are those whose `next_event_position` is 0. From
   there, before the first GTID event, only a non-transactional rotation
   chain may follow:
   - **The first real event starts exactly at the anchor offset**, in the
     anchor's file (`next_event_position - event_length`). It is either:
     - the GTID event, or
     - the file's own Rotate event (a frontier at the end of a file),
       possibly after a Stop event (a server that stopped there).
   - **Each Rotate follows the server's normal file sequence:** same base
     name, file index plus one, position 4. Then come the new file's
     Format_description and Previous_gtids events, and possibly
     heartbeats. No other file, no backwards step, no skipped index.
   - **Nothing transactional comes first.** Any query event (`BEGIN`
     included), table map, rows event, Xid, transaction payload or
     anonymous GTID before the first GTID event is refused. An Incident is
     refused as today.
   - **The first GTID must not be in `set_before`.** When `next_gtid` is
     known, it must be that GTID.
   - **The effective physical coordinate becomes the observed GTID event's**
     file and start offset. The anchor cache and the record frontier are
     updated from it once the scan commits.

   Anything else is refused with `AnchorMismatch` before any statement
   counts and before any state changes:
   - a `BEGIN` without a GTID (probe 3: an offset inside a transaction);
   - a real event starting elsewhere than the anchor offset;
   - a Rotate out of sequence;
   - another GTID, or an in-set one;
   - an artificial Rotate naming another position.
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
| The file rotated since (still present) | Valid. When the anchor is a transaction start inside the older file, the first real event is its GTID event, and the scan crosses the rotation later (probe 5). When the anchor is a frontier at the older file's end, the chain is: the file's Rotate at the anchor offset, the next file's setup events, its first GTID. Both pass the boundary check, and the effective coordinate becomes the GTID's file and offset. |
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

### Bounded request coalescing, revision 2 (final for review)

Revision 2 replaces revision 1's coalescing section, using probe 7.

**Why two layers.** The source is one task: it reads an event, advances
its position, then dispatches it (`mod.rs:1940-1955`). A row of an
unproven table is proved inside the dispatch, with the stream paused, so
proofs never overlap. Two hundred "simultaneous" first touches are 200 rows
arriving close together in one ordered stream. Coalescing them takes:

- **Layer 1, an activation window:** it turns a burst of first touches into
  one batch.
- **Layer 2, a coalescing scanner:** it answers every request of a batch,
  and any concurrent requests, from one physical extension.

#### Layer 1: the activation window

**Entry.** Before advancing the position of a rows event, the loop checks
whether its table is unproven. The check is a cache lookup, the same one
`select_for_rows` makes. If it is unproven, the loop opens a window instead
of dispatching:

1. **Read ahead.** The window reads further events from the stream into an
   in-memory lookahead queue, without advancing the position or
   dispatching. `read_next_event` serves that queue first afterwards. For
   each buffered event it tracks:
   - table-map events, for the tables' names (without touching the
     dispatcher's `table_map`);
   - transaction boundaries, keeping a running exact GTID set: A_head,
     plus each completed buffered transaction's GTID, inserted exactly;
   - rows events of unproven tables not yet in the batch. Each joins the
     batch with its own boundary A_i, the running set before its
     transaction.
2. **Close.** The window closes at the first of these:
   - `activation_window_ms` (default 50 ms) after its head event was
     received;
   - `activation_window_events` buffered (default 10,000);
   - `activation_window_bytes` buffered (default 16 MiB);
   - a statement that can change a schema (any query event other than
     transaction control); the window closes before it;
   - a heartbeat (the stream is idle, so nothing more is coming).
3. **Batched capture.** One read-only transaction locks every batch
   table's metadata in sorted `(schema, table)` order (probe 7: any other
   order can deadlock against multi-table DDL). It then reads the position,
   all the batch's schemas in one loader query (`WHERE (TABLE_SCHEMA,
   TABLE_NAME) IN (...)`), and the position again. Each table's lock is
   held across both reads, so a table's capture is exactly today's capture.
   The B for every table of the window is the second read.
4. **One scanner request.** Interval (A_head, B]. Every table i is answered
   over its own (A_i, B] from the extended record (layer 2), with its own
   membership and digest. Since A_head ≤ A_i, nothing more is scanned.
5. **Dispatch.** Each table is admitted or refused exactly as today, from
   its own answer (`covers`, the relevant-DDL retry and the full fallback
   are unchanged). Then the head event and the queue are dispatched in
   order. Output order and checkpoints are untouched: nothing is emitted
   until its table is decided, as today, and the position advances only as
   events are dispatched.

**Outcomes.** A window either admits its batch as a whole or admits
nothing, and every path is bounded:

| Case | Handling |
|---|---|
| The batched capture's lock wait times out (1205: a DDL in progress on a batch table), or deadlocks (1213) | Retried in sorted order, at most **three batched-capture attempts in total** per window. If none succeeds, the window **fails closed** with a typed `mysql_capture_contention` incident: the lookahead queue stays undispatched (then discarded), the checkpoint stays at the boundary before the window, and the pipeline recovers only through its normal recovery policy. There is never a fallback to per-table captures, which could add one transaction per table of the window. A bounded subdivision would need a design of its own. |
| The shared extension fails, or is refused after its attempts (layer 2) | **No table of the window is admitted** from that request, and none is marked proved. The window fails closed like a capture failure, with the scanner's error, and there are no per-table rescans. The record is unchanged (staging). |
| The shared extension succeeds | Every table receives its own answer over (A_i, B] from the committed record. Each is then decided exactly as today: admitted, refused because its rows contradict the proven shape, or sent to the existing relevant-DDL path when its answer contains a DDL relevant to it. The window closes before any schema-changing statement, so a relevant DDL in (A_i, B] can only come from a statement committed between the window's close and the capture. The relevant-DDL path therefore runs for at most one table per such statement (its own captures are bounded as today), never for every table of the window. |
| The stream is lost during read-ahead or the prefetch | The queue and the held events are discarded. The position was never advanced past the head event, so the existing reconnect resumes from the last boundary (`abandon_open_transaction`). |
| Cancellation (stop) | The queue is discarded, and the checkpoint is the last dispatched boundary. |

**Prefetch during the admission.** Steps 3 to 5 do not need the stream,
so it is read while they run: events that arrive are held raw, with the
time each was received, and nothing about them is decided, dispatched or
advanced. `read_next_event` serves them after the window's queue and
before the stream. A read error ends the prefetch and is raised where the
stream is read next, after the held events. The next window's time bound
counts from its head event's receipt, and it takes the held events
received within that bound without waiting: tables first touched while a
window was admitted have already waited, and are not held for a second
bound. A dropped stream (`abandon_open_transaction`) discards held events
with the queue; the resume reads them from the server again. Rotation
applies only when nothing is held. Without it, measured on the burst at
offset 0, the tables of the second window of a burst waited for the
first window's whole admission and then a full bound of their own:
the slowest activation decile reached 2.01x the fastest.

**Memory.** The queue is bounded by `activation_window_bytes` and
`activation_window_events`, apart from the channel, and so are the held
events. A window holds raw events only, no decoded rows.

#### Layer 2: the coalescing scanner (state machine)

The existing scanner keeps its record `Covered { lo, hi, statements }`,
its pins and its identity check. Revision 2 adds or changes:

- **Pinning before waiting.** A request registers a pin on its A (and the
  window pins A_head) before it waits for the scanner (existing). Pruning
  never drops a statement at or after the lowest pin, queued or running.
- **Rounds, one at a time.** A round's targets are fixed when it starts:
  - the right target is the greatest B among the requests queued before
    it started;
  - the left target is the least A among them, when that A is before the
    record's `lo`.

  A request queued while no round runs joins the round that starts next.
  A request queued while a round runs joins **exactly the next round**,
  never a later one. So every request is served by the first round that
  starts after it is queued, and a continuously advancing frontier cannot
  starve it. A batch from layer 1 is one request with many answers.
- **A round's physical work** is at most two extensions, one left and one
  right. Each extension makes:
  - **at most one positioned attempt**, at its anchor: the right one at the
    record's frontier anchor, the left one at the requesting boundary's
    stream anchor. There is none when no anchor exists (a snapshot
    baseline, failover drift);
  - **at most one bounded unanchored attempt** (the bounded alternative,
    under `max_seek`): only when there is no anchor, or after its
    positioned attempt was refused (`AnchorMismatch`, `NotReached`, a
    purged file) or failed;
  - **no third attempt.** If the attempts do not reach the target, the
    extension fails, and with it the round, for all its waiters.
- **Staging; no partial commit.** A round builds its extensions into a
  staging record. The shared record, its frontier anchor and its bounds
  change only when every extension of the round has reached its target.
  A failed or cancelled extension leaves them exactly as they were. Today
  the left extension mutates the record before the right one runs
  (`mysql_proof_scanner.rs:275-280`); that changes.
- **Shared work and cancellation.** The physical scan of a round runs on
  the scanner's own task, not on a waiter's. A waiter that is cancelled
  unpins itself and leaves. The round continues while any waiter needs it,
  and is aborted (its connection reset, nothing committed) only when none
  does.
- **Answers.** Every request, and every table of a batch, is answered from
  the committed record over its own (A, B] (`answer`, as today). Each gets
  its own membership and its own digest, statement- and digest-identical
  to a direct scan.
- **Bounds.** The record's size bounds (`RecordLimits`) are checked on the
  staging record before it is committed. Exceeding them fails the round
  for all its waiters and clears nothing that was committed before.

#### Exact bounds

Counted separately, and each traced per request and per window:

| Quantity | Normal path (valid anchors) | Worst case (refused anchors, no anchor) |
|---|---|---|
| Capture transactions per window (`capture_attempts`) | 1 | 3, then the window fails closed |
| Rounds that serve a request | exactly 1 (the first to start after it is queued) | exactly 1 |
| Committed extensions per round (`extensions_committed`) | at most 2 (left and right); normally 1 (right only, when the record already reaches A_head) | at most 2, or 0 when the round fails |
| Positioned attempts per round (`positioned_attempts`) | at most 2 | at most 2 |
| Unanchored attempts per round (`unanchored_attempts`) | 0 | at most 2 (one per extension) |
| Stream openings attributable to one request or window (`stream_openings`) | at most 2; normally 1 | **at most 4** |
| Unanchored attempts per snapshot, failover or refused anchor | - | at most 1 per extension; each yields a session anchor for later requests |

A request that waits for a running round has no openings attributed to it
for that round; the round's openings are attributed to the requests that
started it.

**Per burst.** The number of windows is at most
`ceil(burst duration / activation_window_ms)`, and at most
`ceil(buffered events / activation_window_events)`. So the burst costs at
most 3 capture transactions and 4 stream openings per window. Normally it
costs 1 capture and 1 or 2 openings per window.

**The study's case.** 200 first touches within the first ~100 ms at 2,000
changes/s span two or three windows. Normally that is 2-3 captures and 2-6
openings; at worst 9 captures and 12 openings. Today it is 200 captures
and 200 scans.

### Acceptance targets

The review's targets, each with a measured check:

| Target | Check |
|---|---|
| Seek work proportional to the interval, not the preceding file | Seek per proof flat (within 2x) at offsets 0, 64, 256 and 960 MiB, small-transaction padding (the sensitivity series) |
| Bounded physical work for a burst | 200 first touches in one window, valid anchors: 1 capture, 1 round, at most 2 committed extensions and 2 stream openings (normally 1). Trace counters `capture_attempts`, `extensions_committed`, `positioned_attempts`, `unanchored_attempts` and `stream_openings` are asserted separately |
| Activation latency flat by order | Activation-order decile p50s within 2x of each other, no linear trend, at 0 and 256 MiB |
| The 256 MiB case meets the budget | 1,000 tables, 20% active, 256 MiB small padding: activation p99 under 5 s (now about 51 s) |
| Identity unchanged | Positioned, GTID-positioned and shared answers statement- and digest-identical over the regression matrix |
| Existing regressions green | Reconnect, failover drift, rotation, purge, cancellation, scan limits, dual coordinate, shared-vs-direct, proof matrix |
| Mutants caught | Disabling the positioned start, or the activation window, fails the performance regression |
| A late or wrong anchor never proves | A boundary after A with a DDL in between: `AnchorMismatch` or `NotReached` within seconds (not the 600 s limit), then the bounded alternative; never a proof that omits the DDL |
| No partial scanner state | Cancellation, a refused anchor and a failed extension (left, right, or both in one round) each leave the record, its frontier and the anchor cache unchanged; the next request equals a direct scan |
| The batched capture is safe | Locks in sorted order (a reverse-order mutant deadlocks against a two-table `RENAME`); a lock timeout is retried, within three attempts in total, then fails closed (see "Capture contention fails closed"); a DDL on a locked table waits for the commit |
| Bounded per window, worst case | With every anchor refused: at most 3 capture attempts, 2 positioned and 2 unanchored attempts, and 4 stream openings; never a third attempt per extension. A mutant allowing a third attempt, or a per-table capture fallback, fails on the counts |
| Rotation at a frontier | `hi` ends at a file's last transaction, and the next transaction starts in the next file. The positioned extension at the frontier passes the chain (the file's Rotate at the anchor offset, the next file's setup events, its first GTID) and continues at the new file's offset. Two mutants must fail: one rejecting the valid rotation, and one accepting a transactional event (a `BEGIN`, a rows event or an anonymous GTID) before the first GTID |
| Capture contention fails closed | With a batch table write-locked throughout: three sorted capture attempts, then a `mysql_capture_contention` incident, the lookahead undispatched and the checkpoint unchanged; no per-table capture |
| Batch outcomes are all or nothing | A failed shared extension admits no table of its window and starts no per-table rescan; a successful one gives every table its own answer and digest, identical to a direct scan of (A_i, B] |
| One round per request | A request queued while a round runs is served by exactly the next round, and its answer is identical to a direct scan |

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

## Revision 4: one atomic state-store batch per window

Status: reviewed; the rulings and the batch contract below are binding.

### Why

Positioned scanning and coalescing remove the seek. Measured on
`proof-seek-fix` (`d1090c02`), small padding at offset 0, 200 first touches:
four or five windows, then every table of a window admitted one at a time.
Per table (medians over 200, instrumented build):

| Step | Time | State-store operations |
|---|---|---|
| `register_captured` (schema registry) | 1.21 ms | about 12: index repair (2 stream reads), hash index, numbering floor (and legacy length), lineage record, marker write, log append, version and hash index writes, high-water write |
| activation `append` | 0.42 ms | about 4: version check, digest check, insert, head update |
| decision (`admit_decisive`) | 0.09 ms | 1 (version hash) |
| timeline lookups | 2 µs | 0 (cached since `d1090c02`) |

Every operation is its own `spawn_blocking` call on the one SQLite
connection, with its own commit: about 0.1 ms each. So admission is about
2 ms a table, serial, in activation order. That is the decile slope
(180 to 539 ms, 3.0x), and 2,000 newly active tables would spread over
about 4 s. Revision 3's optimizations removed the duplicate timeline read
(2.35 to 2.03 ms a table) and made the window's locks one request (capture
122 to 73 ms for about 150 tables); they cannot remove a per-table cost of
about 16 round trips.

### The change

Admission moves from each table's first row to the window's close, and all
of a window's durable writes commit in one state-store transaction.

1. **Decide in memory.** After the batched capture and the shared scan,
   for each prepared table in batch order, the source computes exactly what
   `establish_at` computes today, from the same inputs:
   - the barrier to bind, `covers`, the relevant-DDL check, the failover
     drift policy (moved forward from the row: see "Equivalence");
   - the registry version the captured shape would receive;
   - the baseline record, its capture identity, and `admit_decisive` on the
     table's timeline plus that record.

   A table whose decision is anything but "admitted" gets no write and no
   prepared proof: its rows take today's path when dispatched (relevant-DDL
   retry, FULL fallback, or the schema error).
2. **One read batch.** Everything the decisions read from the store (each
   key's registry head, hash index, numbering floor, the lineage record,
   each table's activation stream and the barriers) is read in one call
   per kind, keys in batch order: a fixed number of round trips per window,
   not per table.
3. **One write transaction.** The registry entries (log append plus its
   index and high-water writes) and the activation records of every
   admitted table are applied as one atomic batch: all of them or none.
   Activation records keep `log_append_if_absent` semantics inside it (an
   identical record already present is fine; different bytes under the
   same identity fail the batch).
4. **Then the caches.** Only after the batch commits: each admitted table's
   validated timeline is installed (with the stream heads the batch
   returned), and the registry cache admits its versions. A failed batch
   installs nothing and discards every batch key's cached timeline.
5. **Dispatch.** The head event and the queue are dispatched as now. An
   admitted table's rows now find their baseline in the timeline
   (`Selection::Proven`) and do no admission work.

### The atomic batch contract

A window's admission is one **atomic batch**: a set of reads, the
preconditions they establish, and the writes decided from them. The
backend operations:

```text
read_batch(reads)            -> results           one consistent snapshot
apply_batch(preconditions, ops) -> Committed { seqs } | Conflict { which }
log_find_capture(ns, key, capture_id) -> Option<(seq, bytes)>
```

**Reads.** `read_batch` returns, in one snapshot, every value a decision
uses: KV values, stream heads (the last sequence of a stream, 0 when
empty), the latest entry of a stream, and a stream's length. Each result
carries what its precondition needs.

**Preconditions.** Every read a decision rests on becomes a precondition
of the commit:

- `LogHead { ns, key, seq }`: the stream's last sequence is still `seq`;
- `KvIs { ns, key, value }`: the key is still absent, or holds exactly
  these bytes.

`apply_batch` checks all of them inside its transaction, before any write.
If any fails it writes nothing and returns `Conflict` naming it. The
caller re-reads, recomputes every decision, and tries again, at most
`BATCH_CONFLICT_RETRIES` (3) times; then the window fails closed.

**Writes.** All in one transaction: all of them commit, or none do.

- `LogAppendIfAbsent { ns, key, capture_id, value }`: every log write
  carries a deterministic capture identity, registry entries included
  (`reg/<version>/<hash>`), so a commit can be read back. Inside a batch
  the identity must be new: the preconditions guarantee it was absent when
  read, and finding it present fails the batch as a conflict.
- `KvPut { ns, key, value }`, where `value` may be a template naming a log
  write of the same batch: the backend renders that write's sequence into
  it (a version index records its entry's sequence).
- `KvMaxSeq { ns, key, of }`: the key becomes the larger of its current
  number and the sequence of log write `of` (the registry's high-water).

**Sequence allocation.** The log writes of a batch receive one contiguous
range of global sequences, in the batch's order, allocated inside the
transaction and invisible until it commits. The order is deterministic:
the window's tables in first-touch order (the stream order of each table's
first rows), each table's registry entry and then its activation record.

- SQLite: one connection serializes all writers, so the inserts of one
  transaction are contiguous.
- PostgreSQL: `BIGSERIAL` is shared by concurrent transactions, so every
  log writer (`log_append`, `log_append_if_absent`, `apply_batch`) takes
  one transaction-scoped advisory lock before allocating. Allocation, and
  therefore commit, order is serialized: a reader never sees a sequence
  before a lower one that commits later.
- Memory: under its lock.

`Committed { seqs }` returns the committed sequence of every log write.
Nothing reads a sequence earlier.

**Ambiguous outcomes.** An `apply_batch` that returns an error may or may
not have committed (a commit whose acknowledgement was lost). The caller
then reads back every log write's identity with `log_find_capture`:

- **none exists:** nothing committed; apply the same batch again (the same
  bytes, the same preconditions);
- **every one exists with exactly the intended bytes:** the original
  commit succeeded; its sequences are the stored ones, and the templated
  KV writes are checked against them. No new sequence is allocated;
- **some exist, or any differs:** an atomicity violation; the window fails
  closed with that error.

Retries of a storage error that is retryable (a dropped connection, a
busy database) re-run this read-back first, at most
`BATCH_STORAGE_RETRIES` (3) times. A batch is never broken into per-table
writes.

**Caches.** The registry cache and the selection caches change only from a
confirmed `Committed` result (or a read-back that confirmed the original
commit), never before and never from a failed or ambiguous attempt. Any
failure discards the cached timeline of every table in the batch.

**One implementation of the semantics, three backends.** SQLite (one
`spawn_blocking`, `BEGIN IMMEDIATE ... COMMIT`), PostgreSQL (one
transaction with the advisory lock) and Memory (one critical section).
A shared conformance suite runs against all three: preconditions,
atomicity, templates, contiguous and deterministic allocation, concurrent
batches, and read-back.

### Where it is used

- The schema registry gains a planning form of registration: from the
  batched reads it computes the version (with the same numbering, floor,
  lineage and repair rules as `register_with_checkpoint`), the entry and
  index writes, and the preconditions, without executing anything. The
  one-table `register_with_checkpoint` stays for every other caller.
- Decisions validate against the registry plus the batch's planned
  versions (an overlay), so a planned baseline validates exactly as it
  would once registered.
- Activation records are the same bytes and identities as today.

### Failover drift

Checked for every table at the window's close, before any decision. A
drift incident from any table stops the whole window: no registry or
activation write of the window commits, nothing of it is dispatched, and
the checkpoint stays at the boundary before it.

One narrow exception: drift outcomes are recorded before the window's
batch, once every table's assessment has succeeded. A table's outcome (its
proof-bound marker, and the live shape registered at the failover
position) is a completed fact of its own, not part of the window, and may
remain after the batch then fails. It never admits a table, authorizes a
row, advances the checkpoint or creates an activation record. The next
attempt reuses it only after validating it against the same lineage
transition and the same schema versions and hashes (the marker check at
the start of every assessment); the window's admission is recomputed in
full.

### Ordering, as an internal change

The global sequence is an allocation identity, not a promise that the same
input receives the same number in another execution. Batched, a window's
registry entries and activation records take one contiguous range in
first-touch order; their content (versions, hashes, capture identities,
positions, proof bindings) is unchanged. Every schema version still
carries its own unique, monotonic registry sequence, which events report
as `schema_sequence`.

### Failure

A batch that cannot commit (a conflict after its retries, a storage error
after its retries, an atomicity violation, a drift incident) fails the
window closed: nothing of it is dispatched, the checkpoint stays before
it, and the caches of its tables are discarded. There is no fallback to
per-table admission.

### Expected effect

Per window, a fixed number of read round trips and one write transaction
replace about 16 round trips a table. For about 150 tables: about 300 ms
of serial admission becomes one commit of roughly 10-20 ms. What remains in
the activation-order spread is the window sequence itself, so the deciles
should fall within 2x. This is an estimate to be measured.

### Regressions and mutants

- Conformance (all three backends): a precondition conflict writes
  nothing; a failure before the transaction, and one midway through its
  writes, leave nothing; a commit whose acknowledgement is lost is
  recovered by read-back with its original sequences (never new ones);
  concurrent batches get disjoint contiguous ranges; allocation follows
  the batch's order; a partial read-back is an atomicity violation.
- Window: a later table's drift leaves no write and no row of any earlier
  table visible; drift outcomes committed before a failed batch are reused
  unchanged on restart while the whole admission is recomputed; no cache changes on any failure; decisions and stored
  bytes equal the one-table path's for accepted and refused tables.
- Mutants: the batch without its transaction (a midway failure leaves a
  partial write); caches updated before the commit; a retry after an
  ambiguous success allocating new sequences; a refused table written;
  earlier tables' drift outcomes written before every table's assessment
  succeeds.
- Then the acceptance series at 0, 64, 256 and 960 MiB.

## Revision 5: overlapping the next window's capture and scan

### Why

The final-binary series met every target but one: one repetition of 12
had an activation-decile ratio of 2.09. The cause is serial admission.
When a large window (about 130 tables) takes 85-120 ms to admit, the
tables first touched meanwhile wait for all of it, then for their own
window's capture (44-81 ms under load) and commit. Those two windows share
no state: their table sets are disjoint and only their decisions and
commits depend on order. Revision 5 overlaps the next window's capture and
scan with the current window's admission. Decisions and durable commits
stay strictly sequential.

### Terms

- **Current window** (C): the window being captured, scanned, decided
  and committed, exactly as in revision 4.
- **Speculative window** (S): the next window, collected from events held
  during C's admission (the prefetch of revision 4's follow-up), whose
  capture and scan run while C is admitted.
- **Held events**: events read from the stream and not yet dispatched:
  C's lookahead queue and the prefetched events, each with its receipt
  time and a held sequence number.

### Invariants

1. **At most two windows.** One current, at most one speculative. A
   speculative window becomes current only when the stream reaches its
   head event (below); only then may a new speculative window form.
2. **Disjoint table sets, by reservation.** Opening C reserves its batch's
   tables; closing S reserves S's. A reserved table cannot join another
   window while its owner exists: a rows event of a table reserved by C is
   not a candidate for S, whatever its position. Reservations end when
   their window's held rows are all dispatched (or the window is
   discarded).
3. **Speculation is read-only.** S may capture schemas (its own read-only
   MySQL transaction) and obtain proof data from the shared scanner. It
   cannot decide a table, write the state store, allocate a sequence,
   change a selection or registry cache, dispatch an event or move a
   checkpoint. Its candidate filter reads a snapshot of the cache's
   proven flags taken when C's admission starts (read-only, with no
   recency change), never the cache itself: a table not flagged is
   captured, even if a full selection would find it proven; such a table
   is dropped at decision time, below.
4. **Decisions are fresh and unchanged.** After C commits and its held
   rows are dispatched in order, the stream reaches S's head event. S's
   batch is then recomputed with the unchanged functions, from the real
   dispatch state (`txn_eval_cp`, `unproven_at`, `head_tables`, `touched`,
   `admit`) over S's held events. That gives the authoritative batch B'
   and each table's start A_i. Every table of B' must be in S's
   speculative batch with an identical A_i; S's capture and scan answers
   are then used for B' only. Otherwise the window fails closed as an
   internal inconsistency; there is no fallback capture. Then
   `admit_batch` re-reads every registry and activation precondition and
   runs the unchanged decision functions, as in revision 4.
5. **Sequences only at commit.** Sequences are allocated only inside the
   window's own atomic commit (revision 4), never during speculation.
6. **Stream order.** Output, admission and checkpoint order remain stream
   order: events are dispatched one at a time, in order, from C's queue,
   then the held events, then the stream. S never dispatches anything;
   its events wait in the held queue.

Why every table of B' is in S's speculative batch: S's candidates are the
allowed, unreserved tables whose rows S's events touch and that are not
flagged proven in the snapshot. Nothing changes the proven state of an
unreserved table between the snapshot and S's decision: C's commit writes
only C's tables, and no event is dispatched between the snapshot and the
end of C's admission. Every table of B' is unproven (so not flagged),
allowed, and either unreserved (so captured by S) or reserved by C (so
excluded from B' by the refusal rule or by being proven, below). The
starts agree because S tracks boundaries with the same `Ahead` state that
C's read-ahead uses, continued from C's last held event, and C's queue is
dispatched before S's head. A difference would be a defect, so it fails
closed instead of being repaired.

### Lifecycle

1. **C closes** (any revision-4 close condition). C's tables are
   reserved. The proven-flag snapshot is taken.
2. **C's admission runs, with the stream read meanwhile.** Every event
   read is held with its receipt time. S's collection runs over the held
   events in order: it continues C's `Ahead` state and finds candidate
   rows. The first candidate is S's head; S's time bound counts from its
   receipt. S closes under the same conditions as any window: time,
   events, bytes, a statement, a heartbeat, framing, or the table limit.
3. **S closes while C is still admitting.** S's tables are reserved and
   its capture and scan start in an owned task: one batched capture of
   its sorted tables (at most three attempts, revision 4), then one
   shared scanner request over its tables' intervals. The task holds
   nothing of `RunCtx`. If C finishes before S closes, S is dropped
   unformed and its events stay held: when the stream reaches them, an
   ordinary window forms over them, with its time bound counted from
   receipt.
4. **C's outcome.**
   - **Committed:** S's task and its held events stay. C's queue and
     then the held events are dispatched in order.
   - **Failed or cancelled:** the C failure rules below.
5. **The stream reaches S's head event.** S becomes current: B' is
   recomputed (invariant 4), S's task is awaited, and its outcome is
   used. A remembered failure surfaces here, in order. A new speculative
   window may now form during S's admission.

### Failure rules

| Case | Handling |
|---|---|
| C fails before or during its commit, or is cancelled (stop) | S's task is aborted: its capture connection is dropped, so the server rolls back its read-only transaction and releases its metadata locks. Every held event and both windows' reservations are discarded. The source resumes from the last dispatched boundary (`abandon_open_transaction`, or the stop path), exactly as for a failed window today. |
| C's commit is ambiguous (acknowledgement lost) | Resolved by revision 4's read-back inside C's admission, before C returns. S cannot decide before that, because it decides only after C has returned and its held rows are dispatched. |
| S's capture or scan fails, before or after C commits | Remembered in S's task result. Nothing surfaces while C is current. When the stream reaches S's head (every preceding window completed, and its rows dispatched in order), the failure surfaces with revision 4's classification: exhausted lock waits or deadlocks are `mysql_capture_contention`, a connection failure a connection error, a scan failure the scanner's error. The window fails closed; there is no per-table capture. |
| C refuses a table, or finds it missing (`NoCapture`) | That outcome governs every held row of the table up to the last held event when C decides, including rows in C's own queue at later positions. They go on without a lazy baseline, exactly as the refused rows do, and are never captured on their own. This also removes today's per-table capture for later rows of a refused table within one window. Rows read after that point are new and may join a later window. |
| A held row of a table reserved by C appears in S's events | Not a candidate for S. After C, the table is either admitted (its later rows select over C's committed timeline) or governed by the refusal rule above. |
| Disconnect (any read error or injected drop) with two windows present | S's task is aborted. The held events, both reservations and the refusal record are discarded. The resume reads the events from the server again. |
| Credential rotation | Deferred while any event is held or any window exists, as in revision 4's follow-up. |

### Locking

Both capture transactions lock their own table sets in the global sorted
`(schema, table)` order. The sets are disjoint, and shared metadata locks
are compatible, so the two captures never wait for each other. A DDL
spanning both sets (for example a multi-table `RENAME`) requests its
exclusive locks in the same global order (probe 7). Every party then
acquires in ascending order and holds only locks below the one it waits
for, so no wait cycle can form. Lock-wait timeouts and deadlocks keep
revision 4's bounds: at most three batched-capture attempts per window,
then a fail-closed `mysql_capture_contention`. No path, including
recovery from a failed or mismatched speculation, makes a per-table
capture.

### Scanner state if C fails

S's scan is an ordinary request on the shared proof scanner. A scanner
extension it caused may remain in the scanner's record when C fails. The
record is source-cache state only: validated binlog facts (statements,
identity, coverage) for the source's own lineage, independent of
admission and checkpoints. It authorizes no table by itself. A later
request still checks its own interval, server identity and digests, as
every request does today. S's answers themselves are discarded with S.

### Memory and bounds

- Windows: at most two at a time.
- Held events: within one window's `activation_window_events` and
  `activation_window_bytes` (S's events are a subset), so with C's queue
  at most two windows' worth of events.
- Captures: one batched capture per window, at most three attempts. The
  speculative capture is that capture; there is never a second capture
  for the same window.
- Scans: one shared scanner request per window.
- Per-table captures: none from any window path.

### Expected effect

Measured on the series, the second window's capture and scan (40-60 ms)
leave the serial path. Tables first touched during a large window's
admission then wait for that admission plus their own window's decision
and commit only. The slowest decile is expected to fall from about 260-330
ms to about 200-270 ms, against a fastest decile of 101-200 ms. That is an
estimate, and the full series decides.

### Regressions and mutants

- **First window succeeds while the second capture is in flight:** both
  admitted in order; S's capture overlapped C's commit (trace).
- **First window fails before commit:** S's capture aborted (a DDL on an
  S table then proceeds at once), nothing of either window dispatched or
  written, the checkpoint at C's boundary.
- **First commit succeeds but its acknowledgement is lost:** read-back
  resolves it before S decides; S's sequences follow C's original ones.
- **Speculative capture fails before, and after, the first commit:** C is
  admitted and dispatched; the failure surfaces only at S's head, with
  its class.
- **The same table appears again while reserved:** its held rows follow
  C's outcome (admitted, or refused without a capture).
- **Disconnect and cancellation with two windows present:** every held
  event discarded, S's task aborted, every row delivered exactly once
  after the resume.
- **DDL spanning both table sets (live):** a multi-table `RENAME` of a C
  table and an S table while both captures run. The source either admits
  both windows or fails closed within three attempts, with no deadlock
  and no per-table capture.
- **Strict row and checkpoint order** across two windows.
- **Bounds:** at most two windows, held events within one window's
  limits, one capture and one scanner request per window, no per-table
  capture.
- **No speculative mutation:** no registry entry, activation record,
  sequence or cache change before S decides, checked against the store
  and the caches.
- **Mutants:** S deciding before C's commit returns; a reservation
  ignored (a C table joins S); a speculative cache write; a held row of a
  refused table captured on its own; S's task not aborted when C fails;
  a mismatched speculation repaired by a fresh capture instead of failing
  closed.
- Then the full final-binary series at 0, 64, 256 and 960 MiB. Every
  repetition must meet the 2x decile bound, with no linear trend and no
  material CPU regression, before mutants or the gate.
