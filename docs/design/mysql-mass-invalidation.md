# MySQL mass invalidation: bounded control-plane work per window

Status: design for review, not implemented. Revision 4.1, 2026-10-26.
- **Revision 2** applied the review of revision 1 and added findings about
  existing code.
- **Revision 3** applies the review of revision 2: a durable batch identity,
  the global lock's scope, bulk-append details, the connection budget under
  idle pools, cache and item limits, snapshot compatibility, a dedicated
  statement-count lane and the order of work.
- **Revision 4.1** corrects revision 4:
  - a per-table aggregate limit completes the no-starvation proof;
  - a read-item cap makes the statement bound exact (54 per plan);
  - the marker sweep is chunked and budgeted.
- **Revision 4** applies the review of revision 3:
  - the commit marker stores the batch's outcome, which decides an ambiguous
    commit;
  - memory is bounded from capture to commit;
  - an oversized timeline fails closed;
  - size errors are classified by the caller;
  - existing state over the new item limit stays readable;
  - the order of work is final.

The rulings are in "Decisions".

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
- **Order of work:** findings 1 and 2 first, in one store-correctness PR
  (the final order is in "Order of work").
- **Ambiguous commits:** a durable commit identity per batch.
- **The global lock:** qualified with a multi-source measurement, its
  trade-off documented, and no per-source scalability claimed.
- **Statement counts:** measured in a dedicated PostgreSQL lane in the gate
  manifest. The shared PostgreSQL service is not changed.
- **Stage 2:** held.

### Review of revision 3

- **Commit identity:** its own correctness PR, immediately after the store
  protocol PR. The marker stores the committed outcome, and later kv equality
  never decides it.
- **Capture bound:** lands with cancellation, as one guarantee.
- **Memory:** bounded from capture through commit, with one deterministic
  prefix.
- **Oversized timelines:** fail closed (`mysql_timeline_too_large`) for rc.1;
  compaction or streaming selection is follow-up work.
- **`state_item_too_large`:** classified by the caller that knows the
  relation.
- **Legacy items over `M`:** stay readable.
- **The dedicated `pg_stat_statements` lane:** approved.

### Review of revision 4

- **Per-table aggregate limit:** 64 MiB from capture through commit; a table
  over it fails closed with `mysql_table_state_too_large`. Prefix selection
  uses complete per-table contributions, and the no-starvation argument rests
  on it.
- **Read items:** 16,384 per plan, beside the 16,384 write ops. A single
  table over the cap fails closed with `mysql_table_registry_too_large`. The
  exact bound is recomputed: 54 statements per atomic plan.
- **The marker sweep:** chunked (1,024 per lock acquisition), budgeted,
  measured, and harmless when it falls behind.
- **Approval:** with these corrections, PR #177 is approved to merge once the
  docs checks and CI pass at the updated head. Implementation follows the
  recorded order.

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
- **Item limit for writes:** one newly written state item (a kv value or a
  log entry) may not exceed `M` = 16 MiB.
  - The store has no existing record limit to reuse (searched: only the
    PostgreSQL source's `max_message_bytes`, an unrelated replication
    setting).
  - The store refuses an item over `M` before any statement with a generic
    error `StateItemTooLarge { size, key_digest }`, where `key_digest` is a
    SHA-256 of namespace and key. It names no table, because the store does
    not know one.
  - The caller that knows the relation classifies it. The window, baseline
    and drift paths raise the incident `state_item_too_large` (HaltedSafe,
    operator action) with `db.table`, the size and the digest.
  - `M` is validated by a test that builds the largest MySQL 8.4 table
    definition the server accepts (4,096 columns, maximal comments, defaults
    and ENUM lists) and asserts that its encoded registration is at most
    `M`/2.
  - A write chunk therefore carries at most `C` + `M` of payload, so the
    largest PostgreSQL message is below 20 MiB plus statement text.
- **Existing items larger than `M` stay readable.** The limit applies only to
  new writes, never to reads, so state written before the limit is never made
  unreadable.
  - A read of a legacy item over `M` is counted
    (`deltaforge_state_oversized_reads_total`) and logged once with its key
    digest and size.
  - It counts inside its table's aggregate (section "Bounded memory"). A
    legacy item that alone pushes its table over `T` = 64 MiB makes that
    table fail closed with `mysql_table_state_too_large`, naming the size and
    key digest, rather than breaking the memory bound. That is the explicit
    refusal, and the upgrade query below finds such items beforehand.
  - The upgrade notes give a read-only query an operator can run before
    upgrading to find any item over `M` (`max(octet_length(val))` per
    namespace).

- **Order:** chunks keep request order, and results are reassembled by
  ordinal.
- **Atomicity:** all chunks run in the same transaction. A partial
  transaction is never visible, because nothing commits before the final
  `COMMIT`.
- **Tests:** failure injection after every statement (an extension of
  `BatchFault`) asserts that nothing is visible from another connection, and
  that read-back reports none.

**Bounded memory from capture to commit.**

The batch budget `W` = 64 MiB bounds every step that accumulates per-table
data, not only the final encoded batch.

- **Per-table aggregate limit:** a table's aggregate contribution may not
  exceed `T` = 64 MiB.
  - The aggregate is everything accumulated for that table from capture
    through commit: its captured schema, the existing versions and lineages
    read for it, its timeline and history reads, its registration and index
    entries, its activation record and its share of the commit outcome.
  - A table over `T` fails closed with `mysql_table_state_too_large`
    (HaltedSafe, operator action), naming the table and its aggregate size.
    It is never cut and retried.
- **Per-plan caps** (fixed constants):
  - **Read items, 16,384:** every requested read item of an atomic plan,
    meaning plan reads of both phases, precondition checks and capture-id
    existence checks;
  - **Read-key payload, 4 MiB:** the parameter payload of those reads;
  - **Write ops, 16,384;**
  - **Encoded payload, `W`:** includes the write parameters' keys.
  - A single table whose read items exceed 16,384 (for example through
    thousands of lineages) fails closed with `mysql_table_registry_too_large`
    (HaltedSafe, operator action). A single table's write ops are at most 7,
    so the write-op cap can never be exceeded by one table.
- **Working memory** for one window is at most `W` + one per-table aggregate
  (`T`, the table being accumulated when the prefix closes) + fixed overhead.
  A legacy item larger than `M` counts inside its table's aggregate and is
  bounded by `T` like everything else.

- **One deterministic prefix:**
  - every accumulating step consumes its input incrementally, in the window's
    first-touch order of tables;
  - accounting is per table: each table's complete aggregate contribution,
    its read items, its read-key payload and its write ops;
  - accumulation stops at the longest prefix of tables whose totals fit every
    per-plan cap (`W`, 16,384 read items, 4 MiB of read keys, 16,384 write
    ops);
  - the first step to stop sets the prefix, and every later step works only
    on that prefix.
- **Schema capture** (`capture_batch`):
  - the reads are ordered by the window's table rank, joining a `VALUES
    ROW(rank, schema, table)` constructor and ordering by rank and ordinal
    position;
  - the column read, by far the largest, is streamed row by row
    (`mysql_async` result streaming);
  - when the next table's rows would exceed `W`, the capture stops consuming
    and the remaining rows are discarded without being kept;
  - the existence, primary-key, prefix and metadata reads run only for the
    prefix tables. They are small per table, and they are accounted the same
    way;
  - a single table over `M` is refused (`state_item_too_large`) as soon as
    its accumulated rows exceed `M`.
  - The metadata locks still cover every window table in sorted order, so
    lock order is unchanged. Locks of the suffix tables are released with the
    capture transaction, like everything else.
- **Store reads** (`read_batch`, `load_timelines`):
  - each array statement returns rows ordered by request ordinal and is
    consumed as a row stream (`tokio-postgres` `query_raw`), so a result set
    is never materialized whole;
  - consumption stops at the prefix; rows beyond it are discarded by the
    driver as they arrive, without being retained;
  - no statement is split for memory reasons, so the statement bound is
    unchanged;
  - SQLite steps its statements row by row with the same accounting.
- **Registration planning:** `plan_registrations` plans tables one at a time
  in rank order, and stops at the prefix. Entries are encoded once and moved
  into the batch, not copied.
- **The suffix leaves no trace:**
  - suffix tables are dropped before any decision, cache mutation or write;
  - they get no decision, so the window's existing cut (`first_uncovered`)
    returns their held rows to the stream;
  - nothing of theirs was recorded, so the next window starts as if they had
    never been planned.
- **Binary-log order is preserved:**
  - the prefix is in first-touch order, so the window's first held row
    belongs to an admitted table;
  - the cut falls before the first row whose table is undecided;
  - rows of admitted tables after the cut return to the stream with it, in
    order, and are dispatched later under their committed decision. Nothing
    is reordered.
- **No starvation:**
  - **every valid table fits alone:** a valid table's aggregate is at most
    `T` = `W`, and its read items, read keys and write ops are within the
    per-plan caps; otherwise it is invalid and halts;
  - **every valid rank-0 table is admitted:** the prefix starts with the
    window's first-touched table, which fits;
  - **every non-empty window admits at least one table** and dispatches at
    least its first row, so the stream advances;
  - **a deferred valid table progresses:** cut from one window, it is the
    first-touched table, rank 0, of the window that starts at the cut, so it
    is admitted there; a valid table is cut at most once in a row;
  - **an invalid table halts explicitly:** a table over `T` or over the
    read-item cap raises `mysql_table_state_too_large` or
    `mysql_table_registry_too_large` when it is accumulated as rank 0, or
    earlier. It never loops through cuts.

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
- **Identity:** each logical batch gets a 128-bit random identity and a
  digest of its canonical request encoding. Both are fixed before its first
  attempt and reused by every re-apply.
- **The marker holds the outcome.** The batch's last write, inside the same
  transaction, inserts one row into a new table `df_batch_commit`:
  - `batch_id` (primary key);
  - `digest`: the canonical request digest;
  - `result`: the encoded `BatchOutcome` the caller needs, which is every
    op ordinal's status (`Inserted`, `AlreadyPresent`, `Applied`) and its
    assigned sequence, if any, in ordinal order;
  - `committed_at`.
  - SQLite gets the same table; the in-memory store keeps a map with the same
    fields.
- **Resolution on an ambiguous response:**
  - **Matching id and digest:** committed. The stored result is returned.
  - **No marker:** not committed; re-apply.
  - **Same id, different digest:** `BatchIdentityConflict`, a classified
    error that is never retried.
  - **Never consulted:** later kv equality. Other legitimate writers may
    have changed kv values since the batch committed.
  - **Diagnostic only:** immutable log captures may be checked and logged on
    a mismatch, but they never decide the outcome.
- **Strict result-size bound:**
  - a batch may hold at most 16,384 ops; `AtomicBatch::validate` refuses
    more before any statement;
  - each op's result encodes as one status byte plus a sequence varint (at
    most 10 bytes), so a result is at most 16,384 × 11 bytes plus a header,
    under 192 KiB;
  - the marker's `result` column is checked against a 256 KiB limit before
    the insert, so an encoding bug fails the batch instead of storing an
    oversized marker;
  - the 16,384-op limit is the same plan-wide write-op cap the prefix rule
    enforces, so a plan's result always fits.
- **Retention:**
  - a commit call has a hard deadline of 5 minutes, covering all its attempts
    and its resolution;
  - markers are kept 24 h;
  - a test asserts that the deadline is enforced and far below the retention.
- **The sweep never monopolizes the store lock:**
  - **chunks:** each chunk is its own short transaction: take the store lock,
    delete at most 1,024 markers older than 24 h (oldest first, by
    `committed_at` through an index), commit. Committing releases the lock,
    so other writers interleave between chunks.
  - **budget:** a sweep runs at most 64 chunks or 1 s of wall time,
    whichever comes first, then yields until the next hourly run. That is at
    most 65,536 markers per run.
  - **metrics:**
    - `deltaforge_store_batch_markers_backlog`, the expired markers left
      after a run, from the chunk that came back short;
    - `deltaforge_store_batch_markers_deleted_total`;
    - `deltaforge_store_batch_marker_sweep_seconds`;
    - the lock hold per chunk, as part of the existing lock-wait
      measurement.
  - **falling behind has no effect on correctness:** a marker is consulted
    only by its own batch id within that call's 5-minute deadline, so an
    expired marker is never read. A backlog only costs storage, and the
    backlog metric makes it visible.
- **Item idempotency:** log capture ids stay the guard against duplicate
  items. They are not the authority on whether the batch committed.
- **Tests:**
  - injected ambiguity after commit across multiple chunks, with the marker
    present, absent and with a wrong digest;
  - **a valid later writer:** after an ambiguous commit, another valid
    transaction changes one of the batch's kv values (for example the
    high-water, or a registry key) before resolution. Resolution must still
    report the original batch as committed, with its stored sequences;
  - an oversized result refused before the insert;
  - 16,385 ops refused by validation;
  - the sweep deletes only expired markers and holds the store lock.

**SQLite and memory.** Both keep their loops: SQLite costs 5-20 µs per
statement in process, and the diagnosis shows no SQLite round-trip defect.
They get the same validation, modes, mapping and read-back rules.

**Exact statement bound per atomic plan.**

**Caps the bound rests on** (fixed constants, section "Bounded memory"):
- 16,384 read items per plan: plan reads of both phases, precondition checks
  and capture-id existence checks;
- 4 MiB of read-key payload;
- 16,384 write ops, `KvMaxSeq` included;
- `W` = 64 MiB of encoded write payload, keys included;
- `I` = 4,096 items and `C` = 4 MiB per statement.

**Packing rule:**
- a group's current chunk closes when it holds `I` items, or once its
  payload has reached `C` (an item is added first, then the check);
- so every chunk closed by bytes carries at least `C`;
- and a non-empty group of `n` items and `B` bytes produces at most
  `floor(n / I) + floor(B / C) + 1` chunks;
- a chunk's payload stays below `C` + `M`.

**Read side: 9 groups.**
- **The groups:**
  - phase 1: `KvGet`, `LogLatest`, `LogHead`, `LogLen`;
  - phase 2: `KvGet`, `LogLatest`;
  - write transaction: `LogHead` and `KvIs` preconditions, and the
    capture-id check.
- **Item-closed chunks:** with Σ n ≤ 16,384, Σ floor(n / 4,096) ≤ 4.
- **Byte-closed chunks:** with Σ B ≤ 4 MiB, Σ floor(B / 4 MiB) ≤ 1.
- **Final chunks:** at most 9, one per non-empty group.
- **At most 14 read chunks.**

**Write side: 4 groups** (`KvPut`, inserts, meta upsert, meta head update).
- **Item-closed chunks:**
  - inserts number at most 16,383 (`KvMaxSeq` takes one op), and each meta
    group has at most one item per insert;
  - Σ floor(n / 4,096) is greatest with every op an insert: 3 + 3 + 3 = 9.
    Any split with `KvPut` gives at most 9.
- **Byte-closed chunks:** with Σ B ≤ 64 MiB, Σ floor(B / 4 MiB) ≤ 16.
- **Final chunks:** at most 4.
- **At most 29 write chunks.**

**Fixed statements: 11.**
- 2 read transactions × 2 (begin, commit);
- the write transaction's begin, lock, sequence reservation, `KvMaxSeq` read
  and write, commit marker and commit.

**Exact bound: 11 + 14 + 29 = 54 statements per atomic plan,** whatever the
table count, schema sizes, lineages or databases. This corrects revision 3's
"45", which assumed one lineage and no read cap.

**What the caps mean for real windows.** A new narrow table needs about 18
read items:
- 7 plan reads;
- 8 precondition checks;
- 2 capture-id checks;
- at most one shared barrier head.

So one plan admits up to 910 new narrow tables, and a larger window commits
its first 910 and cuts the rest to the next window.

| Case | Tables per plan | Read statements | Write statements | Total per plan |
|---|---|---|---|---|
| Measured narrow tables (1.5 KB entry, k = 0, all new) | up to 910 | 6 | 14 | about 20 |
| 100-column tables (about 30 KB entry, all new) | up to 910 (about 28 MiB) | 6 | about 20 | about 26 |
| Any tables, any sizes | prefix fitting every cap | at most 18 | at most 36 | at most **54** |

A 2,048-table narrow window therefore takes three plans: about 60 statements.
Today it takes about 30 statements per table, about 61,000.

- **What the bound depends on:** the fixed caps, not the number of tables
  invalidated. More tables mean more plans of at most 54 statements each,
  one per window, and each plan admits at least one table.

### 2. Batch construction

- **Barrier preconditions:** a `(ns, key, expected head)` appears once per
  batch. Two different expectations for one key are a planning error, refused
  before commit.
- **High-water:** `KvMaxSeq` is applied once with the batch's maximum. It is a
  max, so this is equivalent to applying it per registration.

### 3. Failover drift by window

- **Assessment (row path):** a window's pending tables are decided from the
  window's own `capture_batch` and each table's `shared_intervals` answer,
  the interval (E, S] its proof starts at: the per-table check's decision
  (`pre_assess`, `proof_start`, `shape_from`, `finish_assess`), with no
  capture or scan of its own. Equivalence tests show the reused inputs give
  the per-table verdict for unchanged, changed, new and later-changed
  tables, an affected interval, a record between F and E, rows before F
  and the halt policy.
- **Recording:** markers (`LogAppendIdempotent`, deterministic identity and
  bytes) and captured registrations are ops of the window's atomic batch.
  When the batch fails, nothing of the drift outcome survives; the restart
  reassesses and records once. An ambiguous commit resolves through the
  batch's marker to the original outcome.
- **One registration per table:** when drift registers a table (the shape
  at F, checkpoint F), that registration is the table's only one in the
  batch and the baseline names its planned version. A window registration
  is merged only when it is the same registration (key, hash and schema);
  any other difference fails the window closed.
- **Snapshot time:** the drift check of a discovery page runs in chunks of
  at most 2,048 tables in discovery order, each with one `capture_batch`,
  one scan of (F, S] and one atomic commit of its outcomes.
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
- **Chunks:** a page's candidates are established in chunks of at most
  2,048 tables in discovery order. Each chunk owns its capture, its S, its
  scan and its atomic commit; the window limits may end a chunk early, and
  its untouched suffix leads the next chunk. A committed chunk stays valid
  when a later one fails.
- **Atomicity:** registrations and baselines of a chunk commit in one batch
  (baselines `LogAppendIdempotent`). Planning registers through
  `plan_registrations`, one batch per chunk.
- **Per-table decisions unchanged:** an uncovered capture interval or a
  relevant statement leaves the table unproven, with no registration or
  baseline from that attempt, as in the per-table path.
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
- **Correction (5c):** a generation is never resumed part-way. A crash
  before the generation completes replaces it (`snapshot_driver::decide`):
  a new generation, a new anchor R0' and a new stream-start barrier there,
  then every table established at R0'. An earlier revision assumed a
  page-level resume; no such path exists, and 5c does not add one.
- **Required tests:**
  - **Replay at the same R0:** establish run again at the same anchor writes
    nothing.
  - **Interrupted generation:** chunks committed, the run failed, the
    generation replaced: every table is established exactly once at R0',
    binding R0''s barrier. A baseline of the old generation (or of the
    per-table path) at R0 does not prove the table at R0'.
  - **Retry after an ambiguous chunk commit:** the commit identity resolves
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
- **Per source:** a pool of at most `P` connections (default 4, valid 2-16,
  `proofs.control_connections`), keyed by tenant and source so a source's
  runs and schema loaders share it. It is DeltaForge's own
  (`mysql_control`), not a `mysql_async::Pool`: that pool opens and closes
  connections internally, so neither a runner permit held for exactly a
  connection's life nor an eviction from outside can be built on it. It is
  used for:
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
  | Capture (window, baseline, drift) | bounded by `capture_lock_wait_secs` overall: the lock-holding slot first (its timeout is `mysql_capture_contention`), then a connection within what remains (its timeout is `mysql_control_plane_saturated`) |
  | Catalog read (schema loader) | 30 s; a retryable connection error |
  | Position query | 5 s; retried by the caller's existing policy |
  | Health and preflight | 5 s; reported as "unknown", not "failed" |
  | Cancellation | never waits on a pool; draws only on the reserve |

- **Fair queueing:** acquisition is FIFO within a class (as built: FIFO
  across all classes, per source and for the runner). Health checks are
  limited to one in flight per source, so they cannot starve captures, and
  captures cannot starve the single health check beyond its timeout.

**As built (5d).**
- A capture takes its lock-holding slot before its connection, so a capture
  waiting for a slot holds no connection.
- Saturation raises the incident where a capture cannot be skipped: an
  activation window's capture (not retried in place) and a failover drift
  capture (never recorded as an unprovable drift). A snapshot baseline or a
  forward proof that cannot capture leaves its table to the lazy proof, as
  for any failed capture.
- A connection is reused only for the same DSN (as a digest) and server; a
  connection opened for no particular server (identity reads that learn the
  server) is budgeted and never reused.
- Outside the budget: the one-shot `deltaforge preflight` command (its own
  process), and public helpers no production path calls.
- Measured on the live suites: a 200-table cold activation opened 2 server
  connections (control peak 1, replication peak 2); a 40-table failover
  window 9 (its startup included; control peak 2); a snapshot generation 13
  at 10, 30 and 90 tables alike (control peak 2, snapshot copy 4).

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
- **Per-timeline hard limit, fail closed for rc.1:** 64 MiB of estimated
  bytes and 100,000 records per timeline. This covers the table's stream plus
  its shared barrier streams, each accounted once.
  - A timeline is loaded as a row stream with its size accounted as it
    arrives. The load stops at the limit, so at most the limit plus one
    record is ever held.
  - A timeline over the limit raises `mysql_timeline_too_large` (HaltedSafe,
    operator action), naming the table and the stream's key digest. The
    source stops before dispatching that table's row.
  - It is never served "uncached": that would still materialize or evaluate
    the whole timeline and break the memory bound.
  - **Follow-up, not part of this work:** timeline compaction, or a streaming
    selector that never materializes a whole timeline, to replace the
    fail-closed limit after rc.1.
  - **Upgrade notes:** a read-only query counts records and bytes per
    activation and barrier stream, so an operator can find a stream over the
    limit before upgrading.
- **The cache bound, stated exactly:**
  - the cache holds at most 256 MiB of estimated bytes;
  - transiently, one timeline load in flight adds at most 64 MiB plus one
    record, because it stops at the limit;
  - so the peak is at most 256 MiB + 64 MiB + one record, never an unbounded
    oversized timeline.
  - Admission evicts least-recently-used entries until the new entry fits,
    before inserting it.
  - A test loads timelines just under and just over the limit into a full
    cache. It asserts that the cache's estimated total never exceeds 256 MiB,
    and that the over-limit load raises the incident after holding at most
    the limit plus one record.
- **Barrier-cache mutation test:** a mutant removes the cache update from
  `record_barrier`, or from each of its two callers (`record_stream_start`
  and DDL handling). Each must be killed by a test that appends a barrier
  and then asserts that a cached table's selection changes accordingly.

### Head-of-line delay after the change

- A window's control-plane cost becomes:
  - one batched capture: MySQL schema work, about 1 ms per table on the
    server;
  - one shared scan;
  - at most 54 store statements per atomic plan.
- Rows still wait for their window in order. The remaining per-table share is
  MySQL's own schema reads.
- **Projection, to be validated by stage 2:** a 40,000-table cold start of
  narrow tables runs about 44 plans of up to 910 tables, giving:
  - about 40 s of schema reads;
  - about 44 × 20, roughly 900, store statements in all, against about 1.2
    million today;
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
   - **rollback after the sequence reservation:** batch A reserves a block
     with `setval` and rolls back. Another batch B commits, then A is retried.
     Every committed sequence is unique and increases in commit order, and
     A's gap is never reused.
5. **Commit identity:**
   - marker present (the stored result returned), absent (re-apply), and
     with a wrong digest (`BatchIdentityConflict`);
   - **a valid later writer** changes one of the batch's kv values after the
     ambiguous commit, and resolution still reports the batch committed with
     its stored sequences;
   - result-size limit and op-count limit;
   - commit-call deadline;
   - **the sweep:** expired markers only; at most 1,024 per lock acquisition;
     the lock released between chunks (a concurrent writer acquires between
     two chunks); the per-run budget enforced with a large backlog; the
     backlog metric reported; and resolution unaffected while a backlog
     exists.
6. **Size limits and memory:**
   - `StateItemTooLarge` carries the size and key digest, and each caller
     classifies `state_item_too_large` with its relation;
   - a legacy item over `M` is still read, counted and accounted;
   - working memory stays within `W` + one maximum item + overhead (measured
     with jemalloc) for windows of 2,048 tables with large schemas, through
     capture, reads, planning and commit;
   - **the deterministic prefix cut, interleaved:** held rows of admitted and
     deferred tables interleave in binary-log order. Admitted tables commit;
     every row is delivered once, in binary-log order; deferred tables are
     admitted by the following windows, with nothing recorded for them before
     their own window;
   - **no starvation:** a valid table whose aggregate is near `T` and that is
     cut from a full window is admitted by the next window as its rank-0
     table;
   - **invalid tables halt:** a table over `T` raises
     `mysql_table_state_too_large`, and one over 16,384 read items raises
     `mysql_table_registry_too_large`, as rank 0. Neither is cut repeatedly;
   - **statement counts:** a plan constructed at every cap at once (16,384
     read items, 16,384 write ops, 64 MiB payload) executes at most 54
     statements in the dedicated `pg_stat_statements` lane;
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
   - idle retention and eviction under pressure: **many sources with idle
     pools fill the runner budget; a new capture request evicts idle
     connections at once and acquires within its timeout; no class starves**;
   - per-class timeouts and saturation metrics;
   - draining on rotation and failover.
9. **Caches:**
   - every invalidation path (section 6);
   - the fail-closed timeline limit (`mysql_timeline_too_large` after holding
     at most the limit plus one record);
   - the cache total never above 256 MiB;
   - memory validation of the defaults.
10. **Findings 1-6:** each first gets a failing regression of its own.
11. **Mutants:**
    - preconditions read before the lock;
    - one guarded writer without the lock;
    - duplicate capture ids accepted with different bytes;
    - `KvMaxSeq` as a sum;
    - read-back ignoring the marker;
    - resolution falling back to kv equality;
    - the prefix not in first-touch order;
    - a suffix table recorded before its own window;
    - a timeline served past its limit;
    - prefix selection using one item instead of the table's aggregate;
    - the read-item cap ignored;
    - sweep chunks larger than 1,024, or several chunks under one lock;
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

1. **Store correctness (findings 1 and 2):**
   - the lock-first protocol for every guarded writer at once, so no writer
     follows the old protocol in between;
   - a separate failing regression for each finding;
   - the multi-source qualification;
   - no bulk execution, no batching.
2. **Durable batch commit identity (finding 6):**
   - the marker table, which stores the outcome;
   - resolution from the stored result;
   - the result-size bound, retention and the sweep;
   - the later-writer regression.
3. **Cancellation and the capture bound (finding 3):**
   - bounded, awaited, logged kills;
   - a guard on every capture path;
   - the four reserved kill permits and the runner-wide bound of four
     lock-holding captures, together.
4. **Stale DSN on rotation (finding 4):** a failing regression first, then
   the fix.
5. **Mass-invalidation batching:**
   - set-based execution, chunking and the dedicated statement-count lane;
   - size limits and bounded memory;
   - failover and snapshot through batched admission;
   - the connection budget;
   - batched histories, cache bounds and the fail-closed timeline limit
     (finding 5).

   It may be split into smaller PRs along those lines, each with its tests.
   Then the diagnosis rerun on both stores (acceptance), then stage 2.
