# MySQL mass invalidation: bounded control-plane work per window

Status: design for review, not implemented. Revision 1, 2026-10-26.

## Problem

Some events invalidate many tables at once. Each table's proof then costs
control-plane work that grows with the number of tables, not with the number
of windows. Rows are delivered in strict binary-log order, so that work stops
the whole pipeline while it runs. Pre-activating tables would move this cost
earlier without changing how it scales, so the cost must first be bounded per
window.

The events:
- a cold start without a resume position;
- a failover to a new lineage;
- a scope barrier (an unattributable statement);
- the end of a snapshot generation.

### Evidence

The activation diagnosis ran on 2026-10-26, on one laptop. It used 1,000
tables with 200 active, 2,000 changes/s, and the runner at `f0171b4e`. Each
expensive scenario ran on both state stores, with the same binary and sizes.

| Per table proved | PostgreSQL store | SQLite store | Backend-independent |
|---|---|---|---|
| Window commit (`commit_ms`) | 6.2-8.3 ms (cold, barrier) | 0.17-0.22 ms | about 30 serial store statements per table |
| Failover drift and commit | 27.6 ms | 11.2 ms | 2 MySQL connections per table, assessed serially |
| Snapshot baselines | 15.2 ms | 9.5 ms | 1 MySQL connection per table, 1 capture per table |
| Return after cache eviction | 1.6 s stall for 50 tables | no stall (146 ms activation) | 4 serial history reads per table |
| Capture (schema and locks) | 0.8-1.2 ms | 0.8-1.2 ms | MySQL server work, inherently per table |
| Scan | 0.1-0.3 ms | 0.1-0.3 ms | one shared scan per window |

Across all scenarios:
- **Correctness:** no row was missing.
- **Application impact:** no probe errors and no lock timeouts. Metadata locks
  were held at most 67 ms per window.

**Conclusion of the evidence:**
- On PostgreSQL, the per-table cost is mostly round trips. There are about 30
  per table, at 0.2-1 ms each.
- On both backends, the connection churn and the serial per-table paths in
  failover, snapshot baselines and history loading are architectural.

Raw data stays local (diagnosis driver, not committed). The figures are quoted
here with their scenario names.

## Where the work is done today

### A. Window commit: about 30 store round trips per table

`admit_batch` (`mysql_activation_window.rs`) runs these steps:

1. **`plan_registrations`** (`adapters/schema_registry.rs`):
   - `read_batch` with 7 reads per table, then a second `read_batch` with 2 or
     more reads per existing version;
   - one snapshot, but PostgreSQL's `read_batch` awaits one statement per read
     (`postgres.rs`, the loop in `read_batch`).
2. **`commit_atomic_counted`, then `apply_batch`:**
   - one SERIALIZABLE transaction under the store's log-allocation advisory
     lock;
   - every precondition and write is its own statement, about 23 per new
     table:
     - preconditions: 7 log heads (registry streams, the activation stream, 2
       barrier streams) and 4 kv checks;
     - writes: 4 kv puts, 2 log appends (each an ensure-meta, insert and
       meta update) and a `KvMaxSeq` read and write;
   - the two barrier-head preconditions repeat once per table in the same
     database (not deduplicated);
   - `KvMaxSeq` on the one high-water row repeats once per registration.
3. **Ambiguous commit read-back:** one `log_find_capture` per log op, and one
   `kv_get` per kv op.

### B. Failover: 2 MySQL connections per table

- `mysql_failover_drift::check_window` calls `assess` serially for each
  pending table, then `record` for each.
- `assess` calls `shape_at_failover`, which runs:
  - `capture()` (`mysql_binlog_scan.rs`): a new `mysql_async::Pool` per call,
    so one new connection with authentication per table;
  - `shared_interval`: one scanner round per table, each extending to the
    right and opening a new replication session.
- `record` writes outside any batch:
  - `register_with_checkpoint`: about 5 `kv_get`, 5 `kv_put`, 3 `log_latest`
    and 1 `log_append`;
  - `write_marker`.

### C. Snapshot: 1 connection and about 10 registry statements per table

- **Baselines:** `mysql_baseline::establish` calls `capture()` once per table,
  serially, with a new pool and connection each time. One shared scan covers
  the page. Then, per table:
  - `register_captured`;
  - a separate activation `append`, which is not atomic with the registration.
- **Planning:** `plan_generation`, through `load_schema`, calls
  `register_version` per table. That is unbatched and auto-committed, about 6
  `kv_get`, 5 `kv_put`, 3 `log_latest` and 1 log append.
- **Owner checks:**
  - `GenerationPublisher::check_owner` reads the control slot once per
    non-empty chunk (28,979 `df_slot` reads for 1,001 tables in the
    diagnosis);
  - that count follows chunks, not tables, and the fixture's sparse ids
    inflated it;
  - it is a fencing read and is out of scope here (see open questions).
- **Copy workers:** they reuse their connections. Copy is not part of this
  defect.

### D. Cache-miss histories: serial per table

- On a miss, `mysql_selection::timeline()` reads, per table and serially:
  - the table's activation stream;
  - the two barrier streams, re-read for every table;
  - one `kv_get` per record;
  - then `get_version` (one more read and a `kv_get`).
- The batched `load_timelines` (`log_read_streams`, one statement) is used only
  at a speculative window's head.
- The selection LRU holds 4,096 entries, at least 2 per table, so it caches
  about 2,000 tables or fewer.

### Connections in general

- **The only long-lived pool:** `MySqlSchemaLoader.pool`, with the crate's
  default limits.
- **Per-call pools or connections:** `capture`, `capture_batch` (one per
  window), the health checks and the control connections.
- **Effect:** under mass invalidation, connections are created per table.

## Goals

1. **Store round trips:** bounded per window and independent of its table
   count, on every backend.
2. **Registry planning:** from batched reads; registrations as bulk writes.
3. **Schema sequences:** allocated and committed atomically in bulk.
   Activation records go in the same transaction.
4. **Failover:** drift assessment and schema capture batched by window.
5. **MySQL connections:** taken from one bounded source-wide pool, never
   proportional to tables.
6. **Snapshot baselines:** through the same batched admission.
7. **Cache-miss histories:** loaded in bounded batches.
8. **Unchanged guarantees:**
   - exactness (the same proofs, the same accounting);
   - strict binary-log order;
   - failure isolation (a window fails whole and nothing of it is dispatched;
     refusals stay per table);
   - ambiguous-commit recovery by read-back.

## Design

### 1. Set-based batch execution in the store backends

The `read_batch` and `apply_batch` interfaces stay as they are: a list of
reads and preconditions/ops in, results in request order out. Only the
PostgreSQL execution changes. It groups items by kind and runs each group as
one array statement, the way `log_read_streams` already works:

- **Reads**, in one `REPEATABLE READ` snapshot:
  - `KvGet`: `SELECT ... FROM unnest($ns, $key) WITH ORDINALITY JOIN df_kv`;
  - `LogLatest` and `LogHead`: a `LATERAL` top-1 per `(ns, key)` over the
    index;
  - `LogLen`: one grouped count.
  - That makes four statements per batch, whatever its size.
- **Preconditions,** in the SERIALIZABLE transaction:
  - log heads and kv checks are evaluated with the same two array reads;
  - each is compared in memory, and the batch returns `Conflict` before any
    write, as today.
- **Writes**, in the same transaction:
  - kv puts: one `INSERT ... SELECT FROM unnest(...) ON CONFLICT DO UPDATE`;
  - log appends and append-if-absent: one `INSERT ... SELECT FROM unnest(...)
    WITH ORDINALITY ... RETURNING seq, ordinality`, still under the advisory
    lock, so sequences stay contiguous and in request order;
  - meta: one `INSERT ... ON CONFLICT` for the meta rows, and one `UPDATE ...
    FROM unnest` for their heads;
  - append-if-absent keeps its semantics: a capture id already present returns
    its existing sequence, through a pre-read and the partial unique index;
  - `KvMaxSeq`: applied once, with the batch's maximum.
- **SQLite:** an in-process connection with cached statements. It keeps its
  loops: its per-statement cost is 5 to 20 µs, and the diagnosis shows no
  SQLite defect here. The in-memory backend is unchanged.
- **Statement count:**
  - about 12 statements per window on PostgreSQL, whatever its table count:
    begin, lock, 2 precondition reads, about 6 writes, commit;
  - a 2,048-table window means larger arrays, not more round trips;
  - array sizes are capped (for example 4,096 items per statement); a larger
    group runs as a few statements in the same transaction.

### 2. Batch construction

- **Barrier-head preconditions:** deduplicated. The same `(ns, key,
  expected)` appears once. Two different expectations for one key are a
  planning error, refused before commit.
- **The high-water mark:** one `KvMaxSeq` per batch, with the maximum of its
  registrations (it is a max, so this is equivalent).
- **`plan_registrations`:** keeps its two read phases, which section 1 makes
  constant in round trips.

### 3. Sequences and atomicity

- **Sequence allocation:** a sequence is still the `df_log` sequence of the
  registration entry. It is allocated by the bulk insert inside the window's
  transaction, so every registration, its high-water mark and every
  activation record commit together or not at all. That is how the window
  path works today; the change is only that it runs as one bulk statement.
- **The paths that are not atomic today:** snapshot planning, baselines and
  drift recording. Each moves onto the same batch (sections 5 and 6).

### 4. Ambiguous-commit read-back

- **Lookups:** `read_back` keeps its decision rules: all found with identical
  bytes means committed; none found means re-apply; partial means atomicity
  violation. Its lookups become two array statements: capture ids against
  `df_log`, and keys against `df_kv`.

### 5. Failover drift by window

`check_window` gets a batched form for the window's pending tables:

- **Assessment:**
  - one `capture_batch` (sorted metadata locks, one connection from the pool);
  - one `shared_interval` request for all tables, so at most the window's two
    stream openings;
  - the same per-table verdicts, decided in memory.
- **Recording:** the drift markers and the captured registrations become ops
  of the window's own atomic batch. A drift incident still stops the whole
  window before any write, as revision 5 of the proof-seek design requires.
- **In-stream path:** `check_in_stream` already reaches drift through the
  window for first-touch rows; any remaining per-row path uses the same
  batched function with one table.

### 6. Snapshot baselines through window admission

- **Baselines:** `establish` captures a page (at most 2,048 tables) with one
  `capture_batch` instead of one `capture` per table. Its registrations and
  baselines then commit in one atomic batch per page, built by the same
  planning functions as `admit_batch`.
- **Planning:** `plan_generation` registers versions through
  `plan_registrations` and one atomic batch per page, instead of
  `register_version` per table.
- **Copy:** unchanged.

### 7. One bounded MySQL connection pool per source

- **The pool:** `MySqlSource` owns one control-plane `mysql_async::Pool` with
  explicit constraints, for example min 0, max 4, configurable and
  validated. It is used for:
  - captures, including `capture_batch` and its `KILL` connection;
  - schema reads;
  - health and preflight checks;
  - positions.
- **Not covered:**
  - replication sessions (the scanner's dump streams) keep their existing
    bounds;
  - snapshot copy workers keep `max_parallel_tables`.
- **Lock isolation:** a capture holding metadata locks keeps its connection
  for the transaction and returns it only after commit or kill, so
  connections are never shared while locks are held.
- **Credential rotation:** rebuilds the pool, as `set_dsn` does today for the
  loader.
- **Result:** connections are bounded by configuration, not by table count.

### 8. Batched history loading on cache misses

- **Prefetch:** on a timeline miss in the row path, the source loads the
  missed table together with the other uncached tables of the current
  lookahead and held events, at most 256. It uses one `load_timelines` call
  (one `log_read_streams` statement).
- **Barrier streams:** cached per stream with their head sequence, and
  invalidated when a barrier is written (the source writes them itself, so
  it knows). They are not re-read per table.
- **The LRU:** sized by tables rather than entries, as a validated setting.
  It is a memory trade-off, recorded with its default.

### Head-of-line delay after the change

- A window's control-plane cost becomes:
  - one capture: MySQL schema work, about 1 ms per table on the server;
  - one shared scan;
  - a constant number of store round trips.
- Rows still wait for their window, in order. The per-table share is now the
  MySQL schema reads themselves, which no design avoids: definitions must be
  read to be proven.
- **Projection, to be validated by stage 2:**
  - a 40,000-table cold start in 2,048-table windows: about 20 windows of
    captures, so about 40 s of MySQL schema work;
  - about 20 × 12 store statements in all;
  - pool-bounded connections;
  - compared with 5-9 min and per-table store and connection work today.

## Invariants and how they are kept

- **Exactness:** the proof functions, selection rules and verdicts are
  unchanged. Only how their reads and writes are executed changes.
- **Ordering:** dispatch is unchanged. A window's rows are released in
  binary-log order after its commit.
- **Failure isolation:**
  - a batch commits whole or not at all;
  - per-table refusals stay per table, because planning and preconditions are
    still evaluated per item;
  - a conflict re-plans the window, as today (at most 3 times).
- **Ambiguous commits:** the same read-back rules, with batched lookups.
- **Lock order:** batched captures keep the sorted metadata-lock order.

## Validation plan

1. **Round-trip tests:** a counting backend wrapper (extending `FaultBackend`)
   asserts that a window's store statements are constant for 1, 64 and 2,048
   tables, on the PostgreSQL implementation through a live server.
2. **Storage conformance suite:** extended with the bulk paths on all three
   backends:
   - append-if-absent idempotency;
   - sequence order;
   - precondition conflicts;
   - read-back after injected ambiguity.
3. **Connection tests:** failover and snapshot end-to-end tests assert that
   MySQL `Connections` grows by at most the pool bound plus the replication
   sessions, independent of table count.
4. **Mutants:**
   - per-item preconditions dropped;
   - duplicated barrier expectations accepted;
   - `KvMaxSeq` taken as a sum;
   - read-back lookups reduced to the first item;
   - the pool bound ignored.
5. **The diagnosis driver,** rerun on both backends (same scenarios and
   sizes). Acceptance: per-table commit and drift costs flat across window
   sizes, and connections bounded.
6. **Stage 2** (10,000 and 40,000 tables) then validates the asymptotic
   behavior directly.

## Open questions for review

1. **Pool bound:** the default and maximum of the control-plane pool, and
   whether it is per source or per runner.
2. **Array cap:** the per-statement array cap, and whether a window larger
   than it may span several statements in one transaction (proposed: yes).
3. **Snapshot owner check:** it is a fencing read per published chunk. It
   could be cached under a slot version and revalidated at a bounded interval.
   That would weaken the fence's granularity, so it is left unchanged unless
   you decide otherwise.
4. **Selection cache:** its sizing by tables, and its memory default.
5. **Order versus pre-activation:** whether bounded pre-activation is still
   wanted after this work. The evidence suggests deciding only after the
   rerun. With round trips and connections bounded, pre-activation would
   mainly hide the MySQL schema-read time, about 1 ms per table.
