# MySQL mass invalidation: bounded control-plane work per window

Status: design for review, not implemented. Revision 2, 2026-10-26.
Revision 2 applies the review of revision 1 (rulings in "Decisions") and adds
the findings about existing code that the review's questions uncovered.

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

## Decisions (review of revision 1)

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

**Chunking.**
- **Caps:** a group is split into chunks of at most `I` items and at most
  `C` encoded parameter bytes; an item larger than `C` forms its own chunk.
  Proposed: `I` = 4,096 and `C` = 4 MiB.
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
- **SQLite and memory:** same validation and mapping; inserts in their
  existing loops.

**Ambiguous-commit read-back.**
- **Lookups:** two array statements per chunk group, capture ids against
  `df_log` and keys against `df_kv`, across all chunks.
- **Rules:**
  - all ops found with identical bytes: committed, with the stored
    sequences;
  - none found: re-apply;
  - some found: `AtomicityViolation`.
- **Tests:** injected ambiguity after commit, with all, none and partial
  results across multiple chunks.

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
  group at most `ceil(n/I) + ceil(B/C)` chunks, and never more than `n`.
- **Statements per window:**
  - phase-1 read transaction: 2 + Σ chunks;
  - phase-2 read transaction, if any: 2 + Σ chunks;
  - write transaction: begin, lock, capture-id check, sequence reservation,
    `KvMaxSeq` (2) and commit, plus Σ chunks over the precondition, `KvPut`,
    insert and meta groups.

| Case | Registrations | Read statements | Write statements | Total |
|---|---|---|---|---|
| Measured narrow tables (1.5 KB entry, k = 0, D ≤ 2,048) | 3 MiB of entries | 7 | 18-19 | about 26 |
| 100-column tables (about 30 KB entry) | about 60 MiB | 7 | about 33 | about 40 |
| Bound for any schema size (every item its own chunk) | any | at most (2 + 7N) + (2 + (2 + k)N) | at most 7 + (4 + k)N + D + 1 + 4N (`KvIs`) + 4N (`KvPut`) + 2N (inserts) + 4N (meta upsert and update) | bounded by the window's 2,048 tables |

  The worst row is reached only when every item exceeds `C`. In every case
  the count is bounded by the window limits (2,048 tables, the configured
  window bytes and events), not by the number of tables invalidated.

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
    when it closes; idle pooled connections expire after a short TTL, which
    returns their permits.
  - Every control-plane connection holds a permit, including cancellation
    connections.
- **Not covered:** replication sessions (the scanner's dump streams) and
  snapshot-copy workers keep their own bounds. Their counts are reported with
  the control-plane count in each source's total connections.

**Cancellation.**
- **Reserve:** `R` of the runner's `G` permits (default 4) are reserved for
  cancellation. A `KILL` connection never waits on any source's pool, so
  lock-holding sessions occupying a pool cannot block their own cancellation.
- **Bounded and visible:** the kill is awaited with a bound (10 s), logged
  and counted when it fails. That fixes finding 3. `lock_wait_timeout` stays
  the server-side backstop.
- **Every capture path** gets the guard, including any remaining single-table
  capture.
- **A probe decides whether the reserve can shrink to zero:** does closing
  the client socket release (a) a session waiting for a metadata lock and
  (b) a session holding one? Only if both are released promptly does the
  design rely on dropping the connection, with the probe's evidence recorded.

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
  - about 20 × 26 store statements for narrow tables;
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
   PostgreSQL, not with a wrapper. The gate's PostgreSQL container gains
   `shared_preload_libraries=pg_stat_statements`; tests reset the counters
   and assert calls per window for 1, 64 and 2,048 tables against the bound
   above. This is a test-infrastructure change to `scripts/gate.sh`.
2. **Concurrency:**
   - overlapping batches;
   - batches against every guarded single-key writer;
   - sequence order under concurrent batches;
   - the high-water staying monotonic.
3. **Failure injection** after every chunked statement: nothing visible, and
   read-back reports none.
4. **Bulk append:**
   - duplicate capture ids within a request, identical and different;
   - existing ids in each mode;
   - ordinal mapping across chunks.
5. **Read-back:** all, none and partial across chunks.
6. **Snapshot equivalence:**
   - the old and new paths write identical bytes and sequences with fixed
     positions;
   - equal under live timing except `to`, `scan_digest` and `capture_id`.
7. **Connections:**
   - MySQL `Connections` growth and peak `Threads_connected` bounded by `P`,
     `G` and the replication bounds, independent of table count;
   - cancellation under a full pool;
   - fairness and timeouts per class;
   - draining on rotation and failover.
8. **Cache invalidation** tests as in section 6; memory validation of the
   cache defaults.
9. **Findings 1-5:** each first gets a failing test.
10. **Mutants:**
    - preconditions read before the lock;
    - one guarded writer without the lock;
    - duplicate capture ids accepted with different bytes;
    - `KvMaxSeq` as a sum;
    - read-back reduced to the first chunk;
    - chunk order reversed;
    - the cancellation reserve removed;
    - a barrier append without cache invalidation.

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

## Open questions

1. **Cancellation reserve:** whether `R` = 4 of 32, or the probe's evidence,
   should decide its final size.
2. **Chunk caps:** whether `I` = 4,096 and `C` = 4 MiB should be configurable
   or fixed constants (proposed: fixed, tested at their edges).
3. **Order of work:** findings 1 and 2 touch the store protocol every source
   uses. Proposed: fix them first, each with its failing test, as their own
   PR, before the batching work builds on the new protocol.
