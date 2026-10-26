# MySQL mass invalidation: bounded control-plane work per window

Status: design for review, not implemented. Revision 3, 2026-10-26.
- **Revision 2** applied the review of revision 1 and added findings about
  existing code.
- **Revision 3** applies the review of revision 2: a durable batch identity,
  the global lock's scope, bulk-append details, the connection budget under
  idle pools, cache and item limits, snapshot compatibility, a dedicated
  statement-count lane and the order of work. The rulings are in
  "Decisions".

## Problem

Some events invalidate many tables at once. Each table's proof then costs
control-plane work that grows with the number of tables. Rows are delivered
in strict binary-log order, so that work holds up the whole pipeline.
Pre-activation would move the cost earlier without bounding it, so it is
deferred.

The events:
- a cold start without a resume position;
- a failover to a new lineage;
- a scope barrier;
- the end of a snapshot generation.

### Evidence

The activation diagnosis ran on 2026-10-26 on one laptop. It used 1,000
tables with 200 active, 2,000 changes/s, and the runner at `f0171b4e`. The
expensive scenarios ran on both state stores, with the same binary and sizes.

| Per table proved | PostgreSQL store | SQLite store | Backend-independent |
|---|---|---|---|
| Window commit | 6.2-8.3 ms | 0.17-0.22 ms | about 30 serial store statements per table |
| Failover drift and commit | 27.6 ms, 415 connections for 200 tables | 11.2 ms, 416 connections | 2 MySQL connections per table, assessed serially |
| Snapshot baselines | 15.2 ms, 1,027 connections for 1,001 tables | 9.5 ms, 1,024 connections | 1 MySQL connection and capture per table |
| Return after eviction | 1.6 s stall for 50 tables | no stall | 4 serial history reads per table |
| Capture (schema, locks) | 0.8-1.2 ms | 0.8-1.2 ms | MySQL server work, inherently per table |

Across all scenarios:
- **Correctness:** no row was missing.
- **Application impact:** no probe errors and no lock timeouts; metadata
  locks were held at most 67 ms per window.
- **Encoded sizes** (4-column tables): a registration log entry is 1.5 KB, an
  activation record 0.3-0.5 KB, a registry index entry 44-115 bytes.

**Two separate defects:**
- **PostgreSQL round trips:** about 30 per table, at 0.2-1 ms each.
- **Backend-independent amplification:** connection churn, per-table snapshot
  baselines, serial failover assessment and serial history loading.

## Findings about existing code

These came out of answering the review. They exist today, independently of
this design, and each should be confirmed by a failing test before it is
fixed.

1. **Batch preconditions read a snapshot taken before the store lock.**
   - **The code:** `PostgresStorageBackend::apply_batch` opens a SERIALIZABLE
     transaction whose first statement is `pg_advisory_xact_lock`.
     PostgreSQL takes the transaction snapshot at that statement, before it
     waits for the lock.
   - **The effect:** a writer that commits while the batch waits is invisible
     to the batch's `LogHead` and `KvIs` preconditions. That includes
     `log_append_if_absent`, which holds the same lock.
   - **Why SSI does not catch it:** serializable conflict detection covers
     only SERIALIZABLE transactions. Every other writer runs at READ
     COMMITTED: `kv_put`, `kv_delete`, `log_append`, `log_append_if_absent`,
     `log_truncate`, the TTL sweep and the slot and queue operations.
   - **Exposure today:** within one source the window commit and the other
     writers of the same streams run sequentially, so a stale precondition
     needs a cross-task writer. Section 1 removes the dependency on that.
2. **The schema-sequence high-water can regress (suspected).**
   - Batched registrations write `__schema_seq_hw` with `KvMaxSeq` inside the
     batch.
   - The unbatched `register_locked` path overwrites it with a plain
     `kv_put` in `bump_hw`, guarded only by an in-process mutex the batch
     path does not take.
   - The registry is shared by every source in a runner. So a slower
     unbatched registration can overwrite a higher value a batch has just
     committed.
   - **Impact:** to be established. The sequences themselves come from
     `df_log`, so the suspected effect is a low starting mirror after a
     restart.
3. **A cancellation kill can silently fail.**
   - `KillOnDrop` spawns a detached task that opens a new connection and
     sends `KILL <id>`. If the connection cannot be opened, nothing is logged
     or retried, and the session keeps its metadata locks until the server
     notices the closed socket or `lock_wait_timeout` expires.
   - The single-table `capture()` has no such guard. It is used by
     baselines, forward proofs and failover drift.
4. **Rotation does not update the source's own DSN (possible gap).**
   - Credential rotation updates `ctx.dsn` and rebuilds the schema loader's
     pool.
   - `MySqlSource.dsn`, which a run start reads, is not updated. Whether a
     run restart inside the same process can then use old credentials is to
     be verified.
5. **Timelines are unbounded and duplicate the lineage barriers.**
   - A cached `Timeline` holds every activation record of its table since the
     lineage began, plus every lineage and database barrier.
   - The lineage barrier vector is copied into every cached table's timeline.
     Nothing compacts these streams.
6. **Ambiguous-commit read-back proves item equality, not this
   transaction's commit.**
   - `read_back` declares a batch committed when every log op is found with
     identical bytes and every kv op holds its value.
   - A later, independent writer could produce the same values, so equality
     does not prove that this transaction committed.
   - Section 1 adds a durable commit identity per batch.

## Decisions

### Review of revision 1

- **Connections:**
  - a per-source control-plane pool: default 4, valid 2-16;
  - a runner-wide control-plane budget: default 32.
- **Statement bounds:** array statements may be chunked inside one
  transaction, capped by item count and by encoded bytes, in request order.
  Bounds are stated as "bounded by the window limits", with an exact worst
  case.
- **Snapshot owner checks:** unchanged. They are fencing reads, reported
  separately.
- **Selection cache:** bounded by tables and estimated bytes: 4,096 tables
  and 256 MiB, whichever comes first, with the defaults validated by measured
  memory.
- **Pre-activation:** deferred until after this work, the diagnosis rerun
  and stage 2. It is then evaluated as a latency or readiness policy.

### Review of revision 2

- **Cancellation:**
  - four reserved cancellation permits for rc.1;
  - a runner-wide maximum of four concurrent lock-holding captures, matching
    the reserve;
  - explicit, bounded cancellation stays mandatory. Dropping the socket can
    become an optimization only after the probe, never a replacement for
    rc.1.
- **Chunk caps:** fixed constants: 4,096 items and a 4 MiB target payload,
  plus a hard, fail-closed limit on one encoded item.
- **Order of work:**
  1. Findings 1 and 2 together, in one small store-correctness PR with a
     separate failing regression for each. No writer is left on the old
     protocol in between, and the PR contains no bulk execution or batching.
  2. Cancellation failure (finding 3), its own correctness PR.
  3. Stale DSN on rotation (finding 4), its own small correctness PR.
  4. The mass-invalidation batching, afterward.
- **Ambiguous commits:** a durable commit identity per batch.
- **The global lock:** qualified with a multi-source measurement, its
  trade-off documented, and no per-source scalability claimed.
- **Statement counts:** measured in a dedicated PostgreSQL lane in the gate
  manifest. The shared PostgreSQL service is not changed.
- **Stage 2:** held.

## Where the work is done today

### A. Window commit

`admit_batch` (`mysql_activation_window.rs`) runs these steps:

1. **`plan_registrations`** (`adapters/schema_registry.rs`):
   - `read_batch` with 7 reads per new table: 2 `LogLatest`, 3 `KvGet`,
     `LogHead` and `LogLen` of the legacy stream;
   - a second `read_batch` with 2 `KvGet` per existing latest version, plus
     one `LogLatest` per other lineage;
   - PostgreSQL's `read_batch` awaits one statement per read.
2. **`apply_batch`,** about 23 statements per new table:
   - preconditions: 7 `LogHead` (legacy, main, side, the table's activation
     stream, 2 barrier streams) and 4 `KvIs`;
   - writes: 4 `KvPut`, 2 `LogAppendIfAbsent` (each an ensure-meta, insert
     and meta update) and a `KvMaxSeq` read and write;
   - the barrier-head preconditions repeat for every table of the same
     database;
   - `KvMaxSeq` repeats per registration.
3. **Ambiguous-commit read-back:** one `log_find_capture` per log op and one
   `kv_get` per kv op.

### B. Failover

- `mysql_failover_drift::check_window` calls `assess` serially for each
  pending table, then `record` for each.
- `assess` calls `shape_at_failover`, which runs:
  - `capture()`: a new `mysql_async::Pool`, so a new connection, per call;
  - `shared_interval`: one scanner round per table, each opening a new
    replication session.
- `record` runs outside any batch:
  - `register_with_checkpoint`: about 5 `kv_get`, 5 `kv_put`, 3
    `log_latest` and 1 `log_append`;
  - `write_marker`.

### C. Snapshot

- **Baselines:** `mysql_baseline::establish` captures each table with its own
  `capture()` and connection, serially. The page then shares one scan over
  (R0, S], where R0 is the generation's anchor and S is the last capture's
  end. Each table then runs `register_captured`, followed by a separate,
  non-atomic activation `append`.
- **Planning:** `plan_generation`, through `load_schema`, calls
  `register_version` per table, unbatched.
- **Owner checks (out of scope):** `GenerationPublisher::check_owner` reads
  the control slot once per non-empty chunk; that is 28,979 `df_slot` reads
  for 1,001 tables in the diagnosis. These reads follow chunk count, not
  table count; the diagnosis fixture's sparse ids inflated them. They are
  fencing reads and stay unchanged.

### D. Cache-miss histories

- On a miss, `timeline()` reads, per table and serially:
  - the table's activation stream;
  - both barrier streams, re-read for every table;
  - a `kv_get` per record;
  - then `get_version`.
- The batched `load_timelines` (one `log_read_streams` statement) is used
  only at a speculative window's head.

### E. Connections

- **The only long-lived pool:** the schema loader's, with crate defaults.
- **Per-call pools:** `capture`, `capture_batch`, the health checks and the
  control connections each create their own.
- **Cancellation:** `KillOnDrop` opens yet another connection.

## Goals

1. **Store statements:** bounded by the window limits, not by table count, on
   every backend.
2. **Registry planning:** from batched reads; registrations as bulk writes.
3. **Atomicity:** sequences allocated in bulk, deterministically, in the
   same transaction as the registrations and activation records.
4. **Failover and snapshot baselines:** through the same batched admission
   as windows.
5. **MySQL control-plane connections:**
   - bounded per source and per runner;
   - cancellation never blocked by the pool;
   - fair between classes of use.
6. **Cache-miss histories:** loaded in bounded batches.
7. **Unchanged guarantees:**
   - exactness;
   - strict binary-log order;
   - failure isolation (a window commits whole or not at all; refusals per
     table);
   - ambiguous-commit recovery.

## Design

### 1. Store transaction protocol (PostgreSQL)

**Serialization (fixes finding 1).**
- **Lock first:** the batch takes the store's log-allocation lock before it
  reads anything. The transaction runs at READ COMMITTED: `BEGIN; SELECT
  pg_advisory_xact_lock(L);`, then preconditions and writes. Each later
  statement takes a fresh snapshot after the lock is held, so it sees every
  transaction that committed before the lock was granted.
- **Guarded data:** every writer of the following takes the same lock for
  its whole transaction:
  - `df_log` and `df_log_meta` (log appends already do);
  - `log_truncate`;
  - the meta ensure;
  - `df_kv` in the registry and activation namespaces (`schemas.v1`,
    `schemas.v1.index` and the activation namespaces).
- **Effect:** mutual exclusion over guarded data. The serialization order is
  the lock order, and a precondition holds at its read and stays true until
  commit.
- **SERIALIZABLE is dropped for batches.** With the lock it adds nothing for
  guarded data, and it produces spurious serialization failures.
- **Writers of guarded keys:**
  - `kv_put`, `kv_delete` and `kv_put_with_ttl` take the lock when their
    namespace is guarded. The guarded list is a constant in the storage crate
    with a test that enumerates every namespace the sources and registry
    write.
  - The registry's unbatched writes, including `bump_hw`, go through the
    lock or through batches. That fixes finding 2: the high-water becomes a
    monotonic max under the lock.
- **TTL:** guarded namespaces never carry a TTL (asserted in `kv_put_with_ttl`),
  so the TTL sweep never touches guarded keys.
- **Proof:** concurrent tests. Two batches over overlapping streams, and a
  batch racing every guarded single-key writer. A precondition that a
  competing commit invalidated must yield `Conflict`, never a commit. The same
  tests run on SQLite (one connection, `BEGIN IMMEDIATE`) and in memory.

**The lock's scope: one lock per store, a documented trade-off.**
- **Why one lock:**
  - `df_log.seq` is one sequence for the whole store, so schema sequences are
    globally monotonic across every source that shares it.
  - The schema-sequence high-water is one key shared by every source in a
    runner.
  - Both need one serialization point for sequence allocation and the
    high-water. Today's log appends already take this one lock; the change
    extends it to the guarded kv writes and to the batch's reads.
- **The cost:** guarded writes of unrelated sources serialize on one lock, so
  the store's guarded write throughput is one critical section at a time. No
  per-source scalability is claimed.
- **Qualified in the store-correctness PR**, before batching, on PostgreSQL:
  - concurrent batches for unrelated sources;
  - lock wait (p50, p99, maximum) and batches per second at 1, 8 and 32
    concurrent sources;
  - no starvation: every waiter acquires within a bound derived from the
    measured critical section, asserted in the test;
  - schema sequences strictly increasing in commit order across sources.
- **If the measurement shows the lock as a bottleneck,** two alternatives are
  recorded for a later decision, neither part of this work:
  - per-source lock keys for per-source data, with only sequence allocation
    and the high-water under the global lock;
  - sequence blocks pre-allocated per source.
- **Batching reduces the lock's hold time:** today's critical section is
  about 30 statements per table. With batching it is bounded by the window
  limits.

**Set-based execution.**
- **Unchanged interfaces:** `read_batch` and `apply_batch` keep their
  signatures: items in, results in request order out.
- **PostgreSQL** groups items by kind and runs each group as array
  statements (`unnest(...) WITH ORDINALITY`), as `log_read_streams` already
  does:
  - **Reads:** `KvGet`, `LogLatest`, `LogHead` and `LogLen` groups, in one
    REPEATABLE READ snapshot. Read batches change no data and keep their own
    snapshot.
  - **Preconditions:** `LogHead` and `KvIs` groups, compared in memory. Any
    failure returns `Conflict` before a write.
  - **Writes:**
    - one bulk upsert per `KvPut` chunk;
    - the bulk log append (below);
    - one meta upsert and one head update per chunk;
    - `KvMaxSeq` once per batch, with the batch's maximum.

**Chunking and size limits.**
- **Caps,** fixed constants: a group is split into chunks of at most `I` =
  4,096 items, with a target payload of `C` = 4 MiB.
- **Item limit:** one encoded state item (a kv value or a log entry) may not
  exceed `M` = 16 MiB.
  - The store has no existing record limit to reuse (searched: only
    PostgreSQL's source `max_message_bytes`, an unrelated replication
    setting).
  - An item over `M` fails closed before any statement, with a classified
    incident `state_item_too_large` (HaltedSafe, operator action). It names
    the table and the size.
  - `M` is validated by a test that builds the largest MySQL 8.4 table
    definition the server accepts (4,096 columns, maximal comments, defaults
    and ENUM lists) and asserts that its encoded registration is at most
    `M`/2.
  - A chunk therefore carries at most `C` + `M` of payload, so the largest
    PostgreSQL message is below 20 MiB plus statement text.
- **Batch byte budget:** a batch's total encoded payload may not exceed `W` =
  64 MiB, which bounds memory.
  - When a window's planned batch would exceed `W`, the window admits the
    longest prefix of its tables in first-touch order whose payload fits. A
    table under `M` always fits on its own.
  - The rest get no decision. The window's existing cut returns their held
    rows to the stream, and a later window decides them. Nothing fails.
  - That cut is the mechanism `first_uncovered` already applies to rows no
    decision covers.
- **Order:** chunks keep request order, and results are reassembled by
  ordinal.
- **Atomicity:** all chunks run in the same transaction. A partial
  transaction is never visible, because nothing commits before the final
  `COMMIT`.
- **Tests:** failure injection after every statement (an extension of
  `BatchFault`) asserts that nothing is visible from another connection, and
  that read-back reports none.

**Bulk append, sequences and capture ids.**
- **Validation, before any statement:**
  - duplicates of a `(ns, key, capture_id)` inside one request with identical
    bytes and digest collapse onto the first ordinal; every duplicate ordinal
    maps to that ordinal's result;
  - duplicates with different bytes or digest are refused with
    `CaptureIdentityConflict`.
- **Existing ids:** one array statement checks every capture id against the
  store. Each op names its mode:
  - **`Absent`** (today's batch behavior): an existing id makes the batch
    `Conflict`. Used by windows.
  - **`AbsentOrIdentical`** (today's single-call `log_append_if_absent`): an
    existing id with identical bytes and digest maps to its existing sequence
    (`AlreadyPresent`); otherwise it is refused with
    `CaptureIdentityConflict`. Used by the idempotent baseline and drift
    paths moved into batches.
- **Sequence allocation:**
  - under the lock, the batch reserves one contiguous block for its new
    entries with `setval` on the `df_log` sequence;
  - it assigns each entry `base + rank`, where rank is the entry's position
    among the new entries in request order;
  - inserts give `seq` explicitly, chunk by chunk, so allocation is
    deterministic across chunks;
  - this is safe because every `df_log` insert takes the lock (verified:
    the three insert sites do).
- **Result:** every input ordinal maps to exactly one `(seq, Inserted |
  AlreadyPresent)`. A rolled-back batch leaves a gap in the sequence, as
  today.
- **Duplicates across chunks:** duplicates are collapsed over the whole
  request before chunking, so a collapsed duplicate never reaches a chunk.
  - A whole-request map from each ordinal to its canonical ordinal is kept,
    and results are reassembled through it.
  - Duplicates whose first and later occurrences would have fallen into
    different chunks therefore map to one result.
- **Interleaved existing and new ids:**
  - existing ids (`AbsentOrIdentical`) map to their stored sequences;
  - new ids take `base + rank`, ranked among new entries only, in request
    order;
  - the outcome is a pure function of the request and the store's state under
    the lock, so it is deterministic.
- **Rollback after reservation:**
  - `setval` is not transactional, so a batch that rolls back after reserving
    leaves its block as a gap;
  - a re-apply reserves a new block, so its sequences differ from the failed
    attempt;
  - nothing may use a sequence before its batch is confirmed (caches are
    installed only from confirmed commits, as today);
  - after an ambiguous outcome, the sequences come from the store, through the
    commit identity below, never from the attempt's reservation.
- **SQLite and memory:** same validation and mapping; inserts in their
  existing loops.

**Durable batch commit identity (fixes finding 6).**
- **The marker:** each logical batch gets a 128-bit random identity and a
  digest of its canonical encoding, both fixed before its first attempt and
  reused by every re-apply.
  - Its last write, inside the same transaction, inserts the marker into a
    new table `df_batch_commit(batch_id PRIMARY KEY, digest, committed_at)`.
  - SQLite gets the same table. The in-memory store keeps a map.
- **Read-back on an ambiguous response:**
  - **Marker present with a matching digest:** committed. The associated
    data is then read back through two array statements (capture ids against
    `df_log`, keys against `df_kv`, across all chunks) to return the stored
    sequences.
  - **Marker absent:** not committed; re-apply.
  - **Atomicity violation:** a marker with a different digest, or a matching
    marker whose associated data is partial or different.
- **Item idempotency:** log capture ids stay the guard against duplicate
  items. They are not the authority on whether the batch committed.
- **Retention:**
  - markers are needed only while their commit call resolves an ambiguous
    outcome, which is bounded by its retries (3 storage retries with
    backoff, seconds);
  - a sweep under the store lock deletes markers older than 24 h, hourly;
  - a test asserts that a commit call's resolution never outlives the
    retention: the call has a hard deadline far below 24 h.
- **Tests:** injected ambiguity after commit across multiple chunks:
  - marker present with complete data;
  - marker absent;
  - marker with a wrong digest;
  - marker present with partial data, simulated by deleting one item before
    read-back.

**SQLite and memory.** Both keep their loops: SQLite costs 5-20 µs per
statement in process, and the diagnosis shows no SQLite round-trip defect.
They get the same validation, modes, mapping and read-back rules.

**Exact worst-case statement bound for a 2,048-table window.**
- **Assumptions:** all N = 2,048 tables are new; k other lineages; D distinct
  databases.
- **Item counts:**

  | Group | Items |
  |---|---|
  | Read phase 1 | 3N `KvGet`, 2N `LogLatest`, N `LogHead`, N `LogLen` |
  | Read phase 2 (existing versions only) | at most 2N `KvGet` and kN `LogLatest` |
  | `LogHead` preconditions | (4 + k)N + D + 1: legacy, main, side and the table's activation stream per table, plus the barrier heads deduplicated (one lineage, D databases) |
  | `KvIs` preconditions | 4N |
  | `KvPut` | 4N |
  | Log appends | 2N, for 2N streams |
  | `KvMaxSeq` | 1 |

- **Formula:** with `I` = 4,096 and `C` = 4 MiB, greedy packing gives each
  group at most `ceil(n/I) + ceil(B/C)` chunks. Here `B` is the group's
  payload, and the batch's total payload is at most `W` = 64 MiB.
- **Reads:** chunked by items. Their results count toward `W`: a read phase
  whose results would exceed `W` cuts the window to a prefix of tables, as
  the write side does.
- **Statements per window:**
  - phase-1 read transaction: 2 + Σ chunks;
  - phase-2 read transaction, if any: 2 + Σ chunks;
  - write transaction: 8 fixed statements (begin, lock, capture-id check,
    sequence reservation, `KvMaxSeq` read and write, commit marker, commit),
    plus Σ chunks over the precondition, `KvPut`, insert and meta groups.

| Case | Registrations | Read statements | Write statements | Total |
|---|---|---|---|---|
| Measured narrow tables (1.5 KB entry, k = 0, D ≤ 2,048, all new) | 3 MiB of entries | 7 | 19-20 | about 27 |
| 100-column tables (about 30 KB entry, all new) | about 60 MiB | 7 | about 34 | about 41 |
| Exact worst case, any schema size (k = 0, D ≤ 2,048, payload ≤ `W`) | ≤ 64 MiB | phase 1: 2 + 2 + 1 + 1 + 1 = 7; phase 2: 2 + 1 = 3 | 8 + 3 (`LogHead`, 10,241 items) + 2 (`KvIs`) + 2 (`KvPut`) + 1 (inserts) + 2 (meta) + 17 (byte splits: `ceil(64 MiB / 4 MiB)` + 1) = 35 | **45** |

- **The worst case** with k other lineages adds `ceil(kN/I)` to the
  precondition and phase-2 groups: 1 each at k = 1, 2 at k = 2, 4 at k = 4.
- **What the bound depends on:** the window limits (2,048 tables, `W`, `I`,
  `C`, `M`), not the number of tables invalidated.

### 2. Batch construction

- **Barrier preconditions:** a `(ns, key, expected head)` appears once per
  batch. Two different expectations for one key are a planning error, refused
  before commit.
- **High-water:** `KvMaxSeq` is applied once with the batch's maximum. It is a
  max, so this is equivalent to applying it per registration.

### 3. Failover drift by window

- **Assessment:** `check_window` assesses a window's pending tables together:
  - one `capture_batch` from the pool (sorted metadata locks);
  - one `shared_interval` request, within the window's two stream openings;
  - per-table verdicts decided in memory by the existing functions.
- **Recording:** markers and captured registrations become ops of the
  window's atomic batch (`AbsentOrIdentical` for the idempotent marker).
- **Incidents:** a drift incident still stops the whole window before any
  write.
- **Connection costs, reported apart:** the failover's reconnection (stream
  open, identity, reconciliation) is reported separately from the new
  lineage's proof work.

### 4. Snapshot baselines through batched admission

- **Boundary preserved:** every table of a generation shares one anchor R0.
  Today every table of a page already shares one S (the last capture's end),
  and each baseline proves (R0, S] with the shared S, provided the table's own
  capture interval lies inside it.
  - With one `capture_batch` per page (at most 2,048 tables), every table's
    capture interval is the batch's interval, and the page's S is that
    interval's end.
  - The same `covers` check and the same scan then prove each table over
    (R0, S].
  - No table's position is ever taken from another: R0 is the generation's
    and S the page's, exactly as today.
- **Fields affected:** only S (`to`), `scan_digest` and the derived
  `capture_id` can differ from a per-table run. They already depend on which
  capture finished last today.
- **Equivalence test:** with positions fixed, the old per-table path and the
  new batched path write identical bytes and sequences. With live timing,
  they compare equal after excluding `to`, `scan_digest` and `capture_id`.
- **Atomicity:** registrations and baselines of a page commit in one batch
  (`AbsentOrIdentical`). Planning registers through `plan_registrations`, one
  batch per page.
- **Unchanged:** copy and owner checks.

**Compatibility across versions and restarts.**
- **Restart safety rests on selection, not on capture ids.** `establish`
  skips every table whose records and barriers already select `Proven` at R0
  (`mysql_baseline.rs`, the candidate loop). That holds whichever path wrote
  the baseline and whatever its S, `scan_digest` or `capture_id`. A baseline
  written by the old per-table path therefore stops the batched path from
  writing a second one, and the reverse holds too.
- **The candidate loop's reads:** two serial store reads per table today.
  They become one `load_timelines` call per page (section 6).
- **Required tests:**
  - **Restart halfway through a generation:** pages 1-2 established, the
    process killed, the generation resumed. Pages 1-2 write nothing, and
    pages 3 onward are established once.
  - **Mixed versions:** pages established by the old per-table binary, then
    the generation resumed by the batched binary. Each table has exactly one
    baseline, sequences are unique, and rows select the same versions as an
    all-old or all-new run.
  - **Retry after an ambiguous page commit:** the commit identity resolves
    it. Committed means no rewrite; absent means a re-apply that writes each
    table once.
  - **DDL between pages:** a DDL on a page-3 table after page 2 commits puts
    the statement inside page 3's scan (R0, S3]. That table gets no baseline,
    as today ("no baseline: affects"), and the other tables are unaffected.
  - **Idempotency:** re-running a completed generation's establish step
    writes nothing.
- **Acceptability:** a different S, `scan_digest` or `capture_id` from the
  per-table path is acceptable only because these tests show mixed-version
  restarts stay correct and idempotent.

### 5. MySQL control-plane connections

**Limits.**
- **Per source:** a `mysql_async::Pool` with constraints min 0, max `P`
  (default 4, valid 2-16, validated config). It is used for:
  - captures and `capture_batch`;
  - schema reads;
  - health and preflight checks;
  - position queries.
- **Per runner:** a budget of `G` permits (default 32, validated).
  - A permit is acquired before a control-plane connection opens and released
    when it closes. Every control-plane connection holds a permit, including
    cancellation connections.
  - `R` = 4 of them are reserved for cancellation (below), so ordinary
    control-plane use draws on `G - R` = 28.
- **Lock-holding captures:** at most `L` = 4 concurrent lock-holding captures
  per runner (a semaphore acquired before a capture's lock statements and
  released at its commit or kill).
  - `L` = `R` guarantees a reserved cancellation permit for every capture that
    can need one.
  - A capture waiting for this semaphore holds no locks. Its wait counts
    toward its `capture_lock_wait_secs` bound.
- **Idle connections:**
  - at most one idle control-plane connection is retained per source, through
    the pool's idle limit; others close when returned;
  - under budget pressure (a permit request that would wait), idle
    connections across the runner are evicted at once to free permits;
  - each idle connection also expires after a short TTL (10 s).
- **Saturation:**
  - every permit acquisition has its class's timeout (below);
  - waits and timeouts are exported as metrics (`deltaforge_mysql_control_permit_wait_seconds`, `deltaforge_mysql_control_permit_timeouts_total` by class);
  - a capture that times out on the budget raises `mysql_control_plane_saturated`
    (AutoRetry), separate from lock contention.
- **Total source connections:** control-plane, replication, cancellation and
  snapshot-copy connections are reported together per source, and each kind
  separately. Replication and snapshot-copy keep their own bounds (the
  window's two stream openings, `max_parallel_tables`), outside `G`.

**Cancellation.**
- **Reserve:** `R` = 4 of the runner's `G` permits are reserved for
  cancellation, matching `L` = 4 lock-holding captures. A `KILL` connection
  never waits on any source's pool or the ordinary budget, so lock-holding
  sessions occupying a pool cannot block their own cancellation.
- **Bounded and visible:** the kill is awaited with a bound (10 s), logged
  and counted when it fails. That fixes finding 3. `lock_wait_timeout` stays
  the server-side backstop.
- **Every capture path** gets the guard, including any remaining single-table
  capture.
- **The probe** (does closing the client socket release (a) a session
  waiting for a metadata lock and (b) a session holding one?) is recorded as
  evidence only.
  - For rc.1 the reserve stays at 4, and explicit, bounded `KILL` stays
    mandatory.
  - Dropping the socket may later become an optimization alongside the kill,
    never a replacement.

**Fairness and timeouts.**
- **Classes and timeouts:**

  | Class | On timeout |
  |---|---|
  | Capture (window, baseline, drift) | bounded by `capture_lock_wait_secs` overall; classified `mysql_capture_contention` |
  | Position query | 5 s; retried by the caller's existing policy |
  | Health and preflight | 5 s; reported as "unknown", not "failed" |
  | Cancellation | never waits on a pool; draws only on the reserve |

- **Fair queueing:** acquisition is FIFO within a class. Health checks are
  limited to one in flight per source, so they cannot starve captures, and
  captures cannot starve the single health check beyond its timeout.

**Draining on rotation and failover.**
- **Rotation:** applied only at a boundary with nothing pending. Today's
  conditions are kept and asserted: no capture holds locks.
  - The source builds the new pool.
  - It swaps it in, so new checkouts use the new credentials.
  - It marks the old pool draining (no checkouts).
  - It awaits the old pool's `disconnect()` with a bound, logging what
    remains.
  - The rotation also updates `MySqlSource.dsn` (finding 4).
- **Failover** (a new server identity): the pool is drained the same way and
  rebuilt for the new server.
  - A capture in flight belongs to a window that the failover aborts. Its
    locks are on the old server; the kill is attempted there through the
    reserve.
  - If the old server is unreachable, nothing more can be done from the
    client; the server's own timeouts apply. This is reported.
- **Permits:** connections of a draining pool keep their permits until they
  close.

### 6. Batched history loading

- **Prefetch:** on a timeline miss in the row path, the source loads the
  missed table together with the other uncached tables of the current
  lookahead and held events, at most 256, with one `load_timelines` call
  (one `log_read_streams` statement).
- **Barrier streams:**
  - cached once per stream (one lineage stream, one per database) and shared
    by reference between table timelines, instead of copied;
  - kept with their heads.
- **Single-writer invariant** (documented in code and tested):
  - a barrier stream's key includes tenant, source id and lineage;
  - the store gate allows one runner per store;
  - source ids are unique within a runner;
  - only two code paths write barrier streams: `record_stream_start` and DDL
    handling, through `record_barrier`.
  - So only the owning source's run loop appends to it. `record_barrier`
    updates or invalidates the cached stream on every append.
- **Invalidation tests:**
  - a database barrier;
  - a lineage barrier;
  - a failover (new lineage keys);
  - a snapshot transition (stream-start barrier);
  - a restart (empty cache, rebuilt from the store);
  - a test that fails if a new barrier writer is added without going through
    `record_barrier`.
- **Selection cache bounds:**
  - 4,096 tables and 256 MiB of estimated bytes, whichever comes first;
  - estimated bytes are the sum of encoded record sizes, the shared barrier
    streams once each, and each loaded schema's encoded size;
  - the defaults are validated by measuring resident memory against the
    estimate (jemalloc) for timelines of 1, 100 and 10,000 records per
    table.
- **Per-timeline hard limit:** 64 MiB of estimated bytes and 100,000
  records.
  - A timeline over either limit is never admitted to the cache. It is served
    from the store on each use: correct but slower.
  - It is counted (`deltaforge_mysql_timeline_uncached_total`) and logged
    once per table, the way the schema registry treats an entry larger than
    its budget (`oversized_uncached`).
  - Shared barrier streams have the same limit, and over it they are not
    cached either.
- **No single item exceeds the budget:** an admitted timeline is at most
  64 MiB, a quarter of the 256 MiB budget.
  - Admission evicts least-recently-used entries until the new entry fits,
    before inserting it.
  - A test admits a timeline just under and just over the per-timeline limit
    into a full cache, and asserts that the estimated total never exceeds
    256 MiB at any point, including transiently.
- **Barrier-cache mutation test:** a mutant removes the cache update from
  `record_barrier`, or from each of its two callers (`record_stream_start`
  and DDL handling). Each must be killed by a test that appends a barrier
  and then asserts that a cached table's selection changes accordingly.

### Head-of-line delay after the change

- A window's control-plane cost becomes:
  - one batched capture: MySQL schema work, about 1 ms per table on the
    server;
  - one shared scan;
  - a store statement count bounded by the window limits.
- Rows still wait for their window in order. The remaining per-table share is
  MySQL's own schema reads.
- **Projection, to be validated by stage 2:** a 40,000-table cold start runs
  about 20 windows of 2,048 tables, giving:
  - about 40 s of schema reads;
  - about 20 × 27 store statements for narrow tables;
  - pool-bounded connections.

## Invariants

- **Exactness:** proof functions, selection rules and verdicts are unchanged.
- **Ordering:** dispatch is unchanged, and a window's rows are released in
  binary-log order after its commit.
- **Failure isolation:**
  - a batch commits whole or not at all, across all its chunks;
  - refusals stay per table;
  - conflicts re-plan, at most 3 times.
- **Ambiguous commits:** the same rules, with batched lookups.
- **Lock order:** batched captures keep the sorted metadata-lock order.
- **Snapshot boundaries:** one R0 per generation and one S per page, as
  today.

## Validation

1. **Statement counts:** measured with `pg_stat_statements` on a live
   PostgreSQL, not with a wrapper.
   - A dedicated lane starts its own PostgreSQL container with
     `shared_preload_libraries=pg_stat_statements`. It is listed in the gate
     manifest (`scripts/gate-suites.txt`) as its own suite.
   - The shared PostgreSQL service and the other suites are unchanged.
   - Tests reset the counters and assert calls per window for 1, 64 and 2,048
     tables against the bound above, separating the test's own queries by
     role.
2. **Concurrency:**
   - overlapping batches;
   - batches against every guarded single-key writer;
   - sequence order under concurrent batches;
   - the high-water staying monotonic;
   - the multi-source qualification (1, 8 and 32 sources: lock wait,
     throughput, no starvation, strictly increasing global sequences).
3. **Failure injection** after every chunked statement: nothing visible from
   another connection; the commit marker absent; read-back reports not
   committed.
4. **Bulk append:**
   - duplicate capture ids, identical and different, within a chunk and
     spanning chunks;
   - ordinal mapping of collapsed duplicates across chunks;
   - existing ids in each mode;
   - existing and new ids interleaved, with deterministic results;
   - rollback after the sequence reservation, then a re-apply.
5. **Commit identity:** marker present and complete, absent, wrong digest,
   present with partial data; markers swept after retention.
6. **Size limits:**
   - an item at and over `M`;
   - the maximal MySQL 8.4 table definition fits under `M`/2;
   - a window over `W` admits its fitting prefix, and the rest is decided by
     the next window.
7. **Snapshot compatibility:**
   - restart halfway;
   - old pages followed by new pages;
   - retry after an ambiguous page commit;
   - DDL between pages;
   - idempotent re-run;
   - fixed-position byte equivalence.
8. **Connections:**
   - MySQL `Connections` growth and peak `Threads_connected` bounded by `P`,
     `G`, `L` and the replication bounds, independent of table count;
   - cancellation with every pool full and `L` captures holding locks;
   - idle retention and eviction under pressure;
   - per-class timeouts and saturation metrics;
   - draining on rotation and failover.
9. **Caches:**
   - every invalidation path (section 6);
   - per-timeline limits and the budget never exceeded;
   - memory validation of the defaults.
10. **Findings 1-6:** each first gets a failing regression of its own.
11. **Mutants:**
    - preconditions read before the lock;
    - one guarded writer without the lock;
    - duplicate capture ids accepted with different bytes;
    - `KvMaxSeq` as a sum;
    - read-back ignoring the marker;
    - chunk order reversed;
    - the cancellation reserve removed;
    - `L` above `R`;
    - each barrier append path without its cache update.

## Acceptance (diagnosis rerun on both stores)

- PostgreSQL store calls bounded by the window limits, not by table count.
- MySQL connection creation and peak concurrency bounded independently of
  table count.
- No serial stall on return after eviction.
- Snapshot baselines and failover through batched admission.
- Unchanged correctness and ordering.
- No worse application-query latency or metadata-lock duration.
- Equivalent stored bytes and schema sequences between the old and new paths
  where ordering is identical.
- Then stage 2 (10,000 and 40,000 tables).

## Order of work

Each step is its own PR, with focused validation and one full gate.

1. **Store correctness (findings 1 and 2), one PR:**
   - the lock-first protocol for every guarded writer at once;
   - a separate failing regression for each finding;
   - the multi-source qualification;
   - no bulk execution, no batching.
2. **Cancellation (finding 3):** bounded, awaited, logged kills; a guard on
   every capture path; the reserve and the lock-holding bound `L`.
3. **Stale DSN on rotation (finding 4):** a failing regression first, then
   the fix.
4. **Mass-invalidation batching:**
   - the commit identity (finding 6);
   - set-based execution and chunking;
   - size limits;
   - failover and snapshot through batched admission;
   - the connection budget;
   - batched histories and cache bounds (finding 5).

   It may be split into smaller PRs along those lines, each with its tests.
   Then the diagnosis rerun on both stores (acceptance), then stage 2.

## Open questions

1. **The commit identity's placement:** it also hardens today's read-back
   (finding 6). It is proposed at the start of step 4. Alternatively it could
   be its own small correctness PR right after step 1.
2. **Placement of `L` and `R`:** proposed in step 2, so that cancellation and
   its capacity land together.
