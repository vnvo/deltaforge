# Durable Snapshot Queue - Design (revision 2)

**Status:** DRAFT for review. Design only: no production code, schema migration or test harness change accompanies this document.
**Date:** 2026-10-05
**Scope:** rc.1 item 3, after `snapshot-paging` (#128), gate reliability (#129) and the snapshot restart fixes (#131).
**Revision 2:** replaces the chunk-resume design of revision 1. A generation is bound to one database read view and is never resumed by another process.

## 0. Rulings this design implements

**Process and scope**
1. One implementation PR covering both engines (the release claim and stored formats are shared), with separately reviewable commits; one core gate on the final accepted tree.

**Generation model**
2. **A generation lives and dies with its database read view.** Worker retry may stay in the generation only while the original view lives:
   - PostgreSQL: while the exporting coordinator transaction is open;
   - MySQL: losing any worker's consistent-snapshot connection replaces the generation.

   A process restart always replaces an incomplete generation. There are no later-view reads and no claim of chunk-granularity crash resume. Reconciliation-window snapshots are a separate post-rc.1 feature.
3. **Sealed plans are immutable.** There is no per-table replanning; a schema that cannot be used fails closed or replaces the whole generation.
4. **Interrupted discovery replaces the generation.** A plan is never continued in a later catalog view.

**Completion and checkpoints**
5. **Completion follows the commit policy**, through a policy frontier:
   - All: the minimum of all sinks;
   - Required: the minimum of the required sinks;
   - Quorum: the position that `quorum` sinks reached.

   A failed optional sink does not hold completion. It gets a non-blocking incident and must be re-bootstrapped. Completion and reclamation semantics are explicit.
6. **The terminal boundary is a first-class checkpoint barrier**, implemented and tested by every sink. The all-empty snapshot passes through the same barrier.

**Legacy, upgrade and rollback**
7. **Legacy progress.**
   - Detection is by explicit format, the lineage is verified, and the replacement is a CAS N to N+1.
   - Work is reset before any row, and old progress or event identities are never mixed into the new generation.
   - Unknown future formats fail closed.
   - The one-time recopy is documented.
   - A legacy snapshot counts as completed only with sink-checkpoint proof; without it, the generation restarts.
8. **Rollback** is safe only once every retained sink checkpoint is an old-binary-readable CDC position.

**Bounds, failures and ownership**
9. **Bounds:**
   - `max_snapshot_connections` enforced by a shared semaphore;
   - an anchor-age bound;
   - a queue (plan) storage bound, named separately from source-log retention;
   - engine-specific WAL and binlog retention checks.

   Crossing a warning threshold raises an incident. Crossing the hard bound halts safely before retention is lost.
10. **Lost anchor retention fails closed** with `snapshot_anchor_unavailable` and requires an explicit, proof-bound `resnapshot`.
11. **Ownership** is detected (not fenced) under the single-writer store-gate contract. The run token is verified before every publish. This is not HA fencing.

**Evidence and acceptance**
12. **Backends:**
    - the queue state-machine and corruption contract runs on both SQLite and PostgreSQL;
    - full engine interruption scenarios run as PostgreSQL source on SQLite, and MySQL source on the PostgreSQL backend.
13. **Acceptance:**
    - progress plus frontier bytes 10K/1K at most 12x, and wall time at most 15x;
    - resident state bounded by page, chunk and worker counts;
    - durable operation counts given by a structurally linear formula;
    - absolute numbers reported;
    - 1K/10K measured once, on the final candidate.

## 1. Starting point (after #131)

**Generation and progress**
- The generation record `snapshot_generation:{source}` holds `generation`, `lineage`, `status` (never advanced in production), `config_fingerprint` and `fingerprint_format` 2.
- Progress is one JSON record per source:
  - PostgreSQL: `start_lsn`, `done_tables`, `finished`;
  - MySQL: `start_position`, `done_tables`, `finished`, `generation`.

  It is rewritten in full after every table. No engine resumes from it any more: both restart an interrupted snapshot in full.

**Snapshot checkpoints (#131)**
- Snapshot boundaries commit an incomplete-snapshot position `{"snapshot":{"format":1,"generation","anchor"}}`.
- Only the completing boundary commits a stream position at the anchor (MySQL: marked `snapshot_completed: g`). It rides the last row the snapshot sent, held back until every table and final check succeeded.
- An all-empty snapshot has no row to carry it, and is copied again until the first change is committed.

**Frontier cost**
- Every boundary's watermark is the full per-table cursor vector, O(tables).
- 10,000 MySQL tables produced 20,001 boundaries (9,614.7 MiB) and 10,002 progress writes (1,236.9 MiB).

**Delivery and resume position**
- One FIFO delivery task; the commit policy (`All`, `Required` default, `Quorum`) is evaluated before any per-sink commit.
- Only event-borne checkpoints are committed; a data-less boundary commits nothing.
- The resume position is the minimum over all per-sink checkpoints, failing closed on any incomparable pair.

**Storage**
- Slots: per-key versions, `slot_create`, `slot_cas`, byte-ordered paged `slot_list`.
- No multi-key atomicity. A deleted and recreated key restarts its version at 1.
- SQLite runs with `synchronous=NORMAL`.

## 2. Model

- A **generation** is one complete initial load over one sealed plan, read in one database read view anchored at `A`.
- **Within a process,** a failed chunk read may be retried only inside the same view:
  - PostgreSQL: a worker may reconnect and import the exported snapshot again while the coordinator transaction that exported it is open; once that transaction ends, the generation is lost;
  - MySQL: each worker's consistent-snapshot connection is its view; losing one loses the generation.
- **Losing the view** (any of the above, or the process ending) **replaces the generation**: the next run allocates `g+1`, plans and anchors again, and copies every table. The single exception is a generation whose terminal barrier the policy frontier already covers: it is completed, not copied again (section 6).
- The durable records exist for four things:
  - bounded memory (the plan is paged from storage, never resident in full);
  - O(1) bookkeeping per boundary;
  - authoritative, policy-based completion;
  - fail-closed classification of whatever state a crash leaves.

  None of them resumes a read.

## 3. Durable records

All records are JSON with an explicit format field. Rules that apply to every record:
- A format above the one this release knows is refused, with the record untouched.
- Generation numbers never repeat, and every per-generation key embeds the generation, so no key is deleted and recreated under one name.
- The source id in keys is escaped as the incidents adapter does.

### 3.1 Generation control record

The existing `snapshot_generation:{source}` record, upgraded in place (`SnapshotStateStore` CAS):

```text
GenerationControl (record_format 3)
  generation         : u64
  lineage            : PersistedLineage
  fingerprint_format : 3              # configuration only; schema is bound per plan item
  config_fingerprint : hex
  state              : allocated | running | rows_produced | completed
  run                : ulid           # the process run that owns this generation (section 8)
  plan               : { sealed: bool, items: u64, bytes: u64, digest: hex }
  anchor             : EngineAnchor?  # set on entering running, never changed
  anchored_at_ms     : i64?           # wall time of the anchor (anchor-age bound)
  terminal           : { digest: hex }?      # set on entering rows_produced
  completed_policy   : All | Required | Quorum(q)?   # the policy that completed it
  replaced           : u64?           # the generation this one replaced (reclamation)
```

`EngineAnchor` is one of:
- `postgres { lsn }`
- `mysql { file, pos, gtid_set, lineage }`

### 3.2 Plan items: the durable per-table state that remains

Namespace `snapshot_plan`. Key: `{source}/{g:016x}/{hex(qualifier)}/{hex(table)}`. Key order equals discovery order, bytewise `(qualifier, table)`.

```text
PlanItem (item_format 1)
  qualifier, table : string
  identity         : [IdentitySpec]
  cursor_kind      : signed | unsigned | ctid_block
  schema_version   : u64        # registry version the plan was made from
  signature        : hex        # whole registered schema model (#128)
```

**Purpose.** Plan items are the only per-table durable state, and they are immutable:
- **Bounded memory.** Workers take items through a paged cursor in key order (page = `discovery_page_size`), so the plan is never resident in full.
- **Anchor verification in pages.** PostgreSQL checks them in the exported snapshot; MySQL checks them under the read lock.
- **The plan digest,** which the terminal digest binds.
- **Operator diagnosis** (`recover diagnose`).

**What they do not hold:**
- no per-table progress, `done` flag, cursor or acknowledgement;
- no `done_tables` set, which is removed;
- nothing per chunk is ever written durably.

**Lifecycle and cleanup ordering:**
1. **Created** by `slot_create` during discovery, while the control record is `allocated` for that generation. An item that already exists with identical bytes is accepted; a different value is a conflict and fails closed.
2. **Sealed:** the control CAS to `running` records `plan.items`, `plan.bytes` and `plan.digest`. No item of the generation is written after it.
3. **Deleted only after** the control record has moved past the generation: a CAS to `completed`, or a CAS replacing it with `g+1`. Deletion is paged and idempotent, and never touches the current generation's prefix.

   After `completed`, diagnosis relies on the control record's plan summary.

4. **On allocation** of `g+1`: if any item key of `g+1` already exists (state lost, so the version-reuse risk applies), the start fails closed.

### 3.3 Progress records

The per-engine progress records (`snapshot_progress:{id}`, `mysql_snapshot_progress:{id}`) are retired; their role moves to the control record:
- anchor: `anchor`;
- finished: `state`;
- generation: `generation`.

They are read only to classify legacy state (section 10), and deleted after the classifying CAS.

### 3.4 Positions and watermarks

**Snapshot positions in sink checkpoints.** Unchanged from #131:
- incomplete: `{"snapshot":{"format":1,"generation","anchor"}}`;
- completing: an ordinary stream position at the anchor, marked `snapshot_completed: g` on MySQL and carrying a `snapshot_completed: g` member on PostgreSQL too, so both engines prove completion the same way.

**Watermark.** For `durable_v2` sinks, O(1):

```text
WmPos::SnapshotSeq { generation, seq, completed }   # WATERMARK_VERSION 2
```

- `seq` is an in-memory publish counter: strictly increasing in publish order within one run, never persisted. A generation is published by one run only.
- The O(tables) `WmPos::Snapshot` vector is no longer produced; it remains readable only to classify legacy state.

### 3.5 Snapshot-to-CDC ordering (precise)

These rules apply only between positions of the same stable lineage; positions of different lineages are always incomparable. `I(g, A)` is an incomplete position, `C(g, A)` the completing position, and `S(p)` a stream position at `p`.

| a | b | order |
|---|---|---|
| `I(g, A)` | `I(g, A)` | Equal |
| `I(g, A)` | `I(g', A')`, `g != g'` or `A != A'` | Incomparable |
| `I(g, A)` | `C(g, A)` | Before |
| `I(g, A)` | `S(p)`, `p` strictly after `A` | Before |
| `I(g, A)` | `S(p)`, `p` at, before or incomparable with `A` | Incomparable |
| `I(g, A)` | `C(g', A')`, another generation | Incomparable |
| `C(g, A)` | `S(p)` | by stream order (`C` is the stream position `A`) |
| `C(g, A)` | `C(g', A')` | by stream order of `A`, `A'` |
| `WmPos::SnapshotSeq(g, s)` | `(g, s')` | by `s` |
| `WmPos::SnapshotSeq(g, ...)` | any other generation | Incomparable, unless the earlier one is `completed` and the other is later |

- Generations are never compared through incomplete positions: a source decides from its control record, not from cross-generation ordering.
- In mixed per-sink states, the resume and frontier computations of section 6 classify each sink separately instead of folding incomparable pairs into an error, where the classification is decidable.

## 4. State machine

| From | To | Durable write | Condition |
|---|---|---|---|
| (none) | `allocated g` | control `slot_create` | lineage checked; no item key of `g` exists |
| `allocated g` | (same) | plan items `slot_create`, page by page | one discovery read view (PostgreSQL repeatable-read session; MySQL rediscovered under the lock at the anchor, #128) |
| `allocated g` | `running g` | control CAS: `plan` sealed, `anchor`, `anchored_at_ms`, `run` | discovery complete in this run; anchor taken; plan verified at the anchor |
| `running g` | `rows_produced g` | control CAS: `terminal` | every item read in this run's view; every worker joined successfully; every final check passed |
| `rows_produced g` | `completed g` | control CAS verifying `terminal` | the policy frontier covers the terminal barrier (6.2) |
| `allocated`, `running` or `rows_produced` `g` | `allocated g+1` (`replaced = g`) | control CAS from the read version | process start finds `g` not `completed` and not coverable; or this run lost its read view; or operator `resnapshot`; or a bound's hard limit (section 9) |
| `completed g` | `allocated g+1` | control CAS | operator `resnapshot`, or mode `always` |

Rules:
- No row of `g` is published unless the control record is `running`, `g` is current and `run` is this run.
- **A start in `rows_produced`** first computes the policy frontier:
  - frontier covers the terminal: CAS to `completed`, no copy;
  - otherwise: replace with `g+1`.
- **A start in `allocated` or `running`** always replaces with `g+1`.
- **A start in `completed`** streams CDC (section 6.3) and never copies rows.
- A replacement deletes nothing before its CAS. Afterwards it deletes the old generation's plan items, and resets the old progress records, if legacy.

## 5. The terminal checkpoint barrier

### 5.1 Contract

A **barrier** is a new delivery operation carried in order with batches: `SourceItem::Barrier { boundary }`. It means three things:
1. Every event published before it is durably delivered: flushed, written, acknowledged.
2. Its checkpoint is durably recorded per sink.
3. Each sink acknowledges it.

The delivery task then handles it like a batch:
- wait for every earlier batch to commit (FIFO);
- call `Sink::barrier(&SinkBatchContext)` on every live sink concurrently, with the same deadline as a batch;
- evaluate the commit policy over the sinks' answers;
- commit the barrier's checkpoint to the per-sink key of every sink that acknowledged it.

A barrier carries no events and is never merged into a batch.

### 5.2 Per-sink implementation (each needs its own test)

| Sink | `barrier` |
|---|---|
| Kafka | flush the producer and await every outstanding delivery report; then acknowledge |
| HTTP, Redis, NATS, ClickHouse, Elasticsearch | `send_batch` already returns only after the sink acknowledged the batch: acknowledge once earlier sends are complete (explicit no-op with a test that a pending earlier send is awaited) |
| S3 `durable_v2` | a manifest entry with no data object, carrying the barrier's watermark, committed by HEAD CAS; acknowledge after the CAS |
| S3 `legacy_rolling` | close and upload the current file; acknowledge (still non-durable, as that mode declares) |

The trait default refuses (`SinkError::Unsupported`). A sink that does not implement the barrier therefore cannot complete a snapshot: the policy decides whether that blocks completion. It is never silently treated as acknowledged.

### 5.3 Use

- After `rows_produced`, the publisher sends one barrier with the completing position and `WmPos::SnapshotSeq { completed: true }`.
- This replaces #131's held-back final event. The all-empty snapshot completes through the same barrier, which removes #131's empty-snapshot recopy residual.

## 6. Policy frontier, completion and resume

### 6.1 Per-sink classification

Every per-sink checkpoint of the source is classified against the control record's current generation `g` and anchor `A`, as one of:
- **behind:** an `I(g, A)`, or a position of an older generation, or none;
- **at-or-past terminal:** `C(g, A)` or a stream position strictly after `A`;
- **foreign:** anything incomparable (another lineage, generation or anchor of the current lineage that is not older, an unknown format).

Foreign fails closed (`snapshot_state_invalid`).

### 6.2 Policy frontier

The frontier covers the terminal when:

| Policy | Covered when |
|---|---|
| `All` | every sink is at or past the terminal |
| `Required` | every `required: true` sink is |
| `Quorum(q)` | at least `q` sinks are |

The `rows_produced` to `completed` CAS records the policy it used (`completed_policy`).

### 6.3 Resume after completion

- **Resume position:** the minimum over the sinks that are at or past the terminal, by stream order. Lagging sinks are excluded from it, so a sink that missed part of the snapshot never pulls the stream back into the snapshot.
- **A sink behind a completed generation** (only possible for a sink outside the policy) raises a non-blocking incident `sink_snapshot_incomplete`, naming the sink and the generation, with action `rebootstrap_sink`. That sink receives no further snapshot rows of `g`; it continues with CDC from the resume position. This is declared, not silent.
- **Reclamation** (deleting `g`'s plan items) happens after the `completed` CAS. It removes nothing a lagging sink could still use, since no snapshot copy of `g` will run again.
- **Changed resume semantics.** Today's fold (the minimum over all sinks, failing closed on incomparable pairs) becomes this classification for snapshot positions. Stream positions still fold by minimum.

## 7. Discovery and plan

- Discovery runs in one read view while the control record is `allocated` (#128 rules), writing items page by page.
- A start that finds `allocated` (plan unsealed, or sealed without an anchor) replaces the generation: a plan is never continued in another catalog view.
- The anchor verifies every item, in pages, against the whole registered schema model (#128), inside the view the rows are read from.
- A sealed plan is immutable: a verification mismatch, or a schema that cannot be used, fails the run. The next start replaces the generation. There is no per-item replanning.

## 8. Ownership

Single ownership per source remains a deployment precondition (the single-writer store-gate contract). This is detection, not HA fencing:
- On entering `running`, the run writes its `run` token into the control record by CAS.
- **Before every publish** (each chunk and the barrier), the publisher reads the control record (one `slot_get`) and requires the current generation, `running` (or `rows_produced` for the barrier) and its own `run`. A mismatch stops the run with `snapshot_state_invalid` (class `concurrent_owner`).
- Two owners can still interleave between that read and the publish. Only a lease closes that window.

The added cost is one read per chunk, which is linear.

## 9. Bounds

| Bound | Resource | Configuration (proposed defaults) | Warning | Hard |
|---|---|---|---|---|
| Snapshot connections | source DB | `snapshot.max_snapshot_connections` (default `max_parallel_tables x max_parallel_chunks + 2`), one shared semaphore over coordinator, lock, workers and intra-table readers | none (it queues) | never exceeded |
| Anchor age | source DB view held open, source log retained | `snapshot.max_anchor_age` (default 24 h) | 80%: `snapshot_bound_warning` (non-blocking) | stop, replace nothing automatically, `snapshot_anchor_unavailable` |
| Plan storage | DeltaForge store | `snapshot.max_plan_bytes` (default 256 MiB) and `snapshot.max_plan_items` (default 1,000,000), checked during discovery | 80%: warning | discovery stops before sealing; the generation fails with `snapshot_bound_exceeded` |
| PostgreSQL WAL retention | source WAL | from the slot: `safe_wal_size` and `wal_status` of the snapshot's slot | `safe_wal_size` below 20% of `max_slot_wal_keep_size`, or `wal_status = unreserved`: warning | `wal_status = lost`, or `safe_wal_size <= 0`: stop with `snapshot_anchor_unavailable` |
| MySQL binlog retention | source binlog | the existing purge guard: the anchor's file and GTID set still on the server; retention age against the anchor age | anchor age above 80% of `binlog_expire_logs_seconds`: warning | anchor file purged or GTID set no longer covered: stop with `snapshot_anchor_unavailable` |

- Queue storage (plan) and source-log retention are separate resources, with separate names and incidents.
- A hard stop never replaces the generation automatically. Recovery is an explicit, proof-bound `resnapshot` (the recovery CLI).

## 10. Legacy state and unknown formats

| Stored | Classification | Action |
|---|---|---|
| Control `record_format` below 3 (any status) **and** the policy frontier covers a completion proven by sink checkpoints | completed legacy | lineage verified; CAS in place to `record_format 3`, `completed`; legacy progress deleted after |
| Control below 3, anything else | incomplete legacy | lineage verified; CAS to `g+1` `allocated` (`replaced = g`); one-time full recopy |
| Unknown `record_format`, `fingerprint_format`, `item_format`, snapshot-position `format` or watermark version | unknown | refused; record untouched; incident before any row |
| Other stable lineage | foreign | refused (`ConfigChanged`); record untouched |

**Proof of a legacy completion comes from the sink checkpoints only**, using the #131 rules:
- PostgreSQL: a stream position at or after the legacy progress anchor (`start_lsn`) for every sink the policy requires, where the anchor is readable; a bare-LSN or incomplete position is not proof.
- MySQL: a `snapshot_completed` mark of the recorded generation at the anchor, or a stream position strictly after the recorded anchor.

`finished = true` and the never-advanced generation status prove nothing.

## 11. Rollback and migration

- **Upgrade** applies section 10 on the first start; nothing is rewritten before a lineage-verified classification.
- **Rollback to a pre-queue release is safe only once every retained sink checkpoint of the source is a CDC position** that release reads:
  - PostgreSQL: a stream position;
  - MySQL: a binlog position.

  That is, after `completed`, and after every sink has committed at least one position past the barrier.
- **Before that, a pre-queue release fails closed:** it cannot parse a snapshot position, and it refuses the format 3 control record on any path that allocates a generation. The operator either upgrades again or runs an explicit re-snapshot.
- **Not supported:** reading queue records with an older release; continuing a queue generation on one.

## 12. Crash ordering

| Step | Write | Crash before | Crash after |
|---|---|---|---|
| Allocate `g` | control create / CAS | nothing allocated; retried | `allocated g` found: replaced by `g+1` |
| Discovery page | plan items | (same generation) replaced at next start | replaced at next start (items deleted after the replacement CAS) |
| Seal and anchor | control CAS to `running` | `allocated`: replaced | `running`: replaced at next start (rows may have been published: duplicates only) |
| Publish chunk | none (O(1) incomplete position per boundary, committed by sinks) | - | replaced at next start |
| Rows produced | control CAS with `terminal` | `running`: replaced | `rows_produced`: frontier check at next start |
| Barrier | per-sink keys (delivery task) | `rows_produced`, frontier not covered: replaced | `rows_produced`, frontier covered: `completed` at next start |
| Completed | control CAS verifying `terminal` | as above | CDC only |
| Reclaim | plan item deletes | items of a past generation remain; deleted later (idempotent) | done |
| Legacy replace | control CAS, then legacy progress delete | classified again | `g+1`; delete repeated |

**SQLite `synchronous=NORMAL`.** A power loss drops a suffix of commits. Every write a later start depends on precedes its externally visible effect:
- the control state precedes rows;
- the terminal precedes the barrier;
- a per-sink key follows the sink's own commit.

A lost suffix can therefore only cause a replacement (duplicates), never loss.

## 13. Invariants (each is a test)

**Before and while rows are published**
- **I1** No row of `g` is published unless the control record is `running`, `g` is current, and `run` is the publisher's.
- **I2** No plan item of `g` is created after `g` is sealed, and none is deleted before the control record has moved past `g`.
- **I3** A start that finds `allocated` or `running` publishes no row of that generation: it replaces it first.
- **I14** A PostgreSQL worker retry inside a generation happens only while the exporting transaction is open. A MySQL worker connection loss replaces the generation.
- **I15** Every snapshot connection is taken from the shared semaphore, and the count never exceeds `max_snapshot_connections`.

**Completion and after**
- **I4** `completed` is reached only by a CAS from `rows_produced` with the identical terminal, and only when the policy frontier covers the terminal barrier.
- **I5** A start in `completed` publishes no snapshot row.
- **I6** A start in `rows_produced` either completes (frontier covered) or replaces. It never publishes a row of that generation.
- **I7** The barrier commits a sink's checkpoint only after every earlier batch was committed for that sink, and only if the sink acknowledged the barrier.
- **I9** After `completed`, the resume position is the minimum over sinks at or past the terminal. A sink behind it raises `sink_snapshot_incomplete` and never pulls the stream into the snapshot.

**State classification and formats**
- **I8** Every per-sink checkpoint is classified as behind, at-or-past, or foreign; foreign fails closed before any row.
- **I10** Generations strictly increase. Allocation refuses when a key of the new generation exists.
- **I11** Legacy classification is by format plus sink-checkpoint proof only. An unknown format or foreign lineage is refused with the record byte-identical.

**Sizes and bounds**
- **I12** Bytes per boundary checkpoint and watermark are independent of the table count, and resident plan state at 10K tables is within a constant of 1K.
- **I13** Each bound's hard limit stops the run before the resource is lost, with its incident, and replaces nothing automatically.

## 14. Test plan

**Queue contract, run on the in-memory backend, SQLite and PostgreSQL** (one shared suite):
- state machine and every refusal;
- crash at every write of section 12, then restart;
- plan item conflicts;
- version-reuse probe at allocation;
- concurrent-owner detection;
- reclamation ordering.

**Additionally:**
- SQLite: corruption of the control record and of an item; restore of an earlier file (lost tail).
- PostgreSQL: CAS contention, and `slot_list` byte order with `COLLATE "C"`.

**Engines (Docker; PostgreSQL source on SQLite, MySQL source on the PostgreSQL backend):**
- interrupted snapshot, then restart: a new generation, full copy, zero loss, `completed` only after the barrier;
- `rows_produced` with the frontier covered at restart completes without copying;
- PostgreSQL coordinator transaction lost while a worker retries: generation replaced;
- MySQL worker connection killed: generation replaced;
- policy matrix (`All`, `Required` with a failing optional sink, `Quorum`): completion, `sink_snapshot_incomplete`, resume position;
- anchor-age hard bound;
- PostgreSQL `wal_status = lost`, and MySQL binlog purged under the anchor;
- plan storage bound;
- legacy completed with proof, legacy without proof, unknown format.

**Sinks:** one barrier test per sink (Kafka, HTTP, Redis, NATS, ClickHouse, Elasticsearch, S3 `durable_v2`, S3 `legacy_rolling`), each with a pending earlier delivery and an all-empty snapshot.

**Delivery task:** barrier ordering, policy evaluation and per-sink commits, under each policy.

## 15. Acceptance measurements

**Structural CI**, on 10, 30 and 90 tables. The probe counts durable writes, durable reads, bytes per boundary and resident maximum.

```text
durable writes = plan items (discovery) + control CASes (<= 5)
               + plan item deletes (items)
durable reads  = control reads (chunks + 1, ownership checks)
               + plan pages (items / page, twice: verification and work)
boundary bytes = constant
progress bytes = 0
```

**Final candidate only:** 1K/10K on both engines on the reference machine (i7-1355U, 12 threads, 31 GB). Absolute times and bytes, peak RSS, boundary counts. Bytes 10K/1K at most 12x, wall time at most 15x, resident state flat.

## 16. Implementation commit sequence (one PR)

1. **Formats and storage:**
   - control record format 3 and plan items in the `snapshot_plan` namespace;
   - classification, allocation, replacement and reclamation;
   - the `WmPos::SnapshotSeq` watermark and its comparator;
   - the shared queue contract suite on the three backends.
2. **Barrier:**
   - `SourceItem::Barrier`, the delivery-task operation and `Sink::barrier`;
   - each sink's implementation and test.
3. **PostgreSQL wiring:**
   - paged plan;
   - generation-scoped view and worker retry;
   - barrier completion;
   - the connection semaphore;
   - the WAL and anchor-age bounds.
4. **MySQL wiring:**
   - the same;
   - the binlog bound;
   - worker-loss replacement;
   - keyless tables chunked by a bounded scan.
5. **Completion and resume:**
   - per-sink classification and the policy frontier;
   - the `rows_produced` start rule;
   - `sink_snapshot_incomplete`;
   - legacy classification with sink proof.
6. **Evidence:** engine scenarios on real backends, structural CI cases, the 1K/10K measurement.
7. **Documentation:**
   - snapshot semantics (no crash resume; replacement);
   - policy completion and lagging sinks;
   - bounds and incidents;
   - upgrade and rollback;
   - capacity envelope;
   - CHANGELOG.

## 17. Old versus new

| Aspect | Revision 1 (rejected) | Revision 2 |
|---|---|---|
| Crash resume | chunk granularity, remaining chunks read in a later view | none: a lost read view replaces the generation |
| Per-chunk durable state | chunk journal per chunk, folded into items | none |
| Per-table durable state | items with acknowledged cursors and pending ranges, updated by fold CAS | immutable plan items (plan paging, verification, digest, diagnosis) |
| Schema change of a pending table | replan that item | never: a sealed plan is immutable; the generation is replaced |
| Interrupted discovery | continue from the last page | replace the generation |
| `rows_produced` at restart, terminal not acknowledged | revert to `running`, resume | replace the generation |
| Completion | per-sink minimum over all sinks covers the terminal | policy frontier (`All`/`Required`/`Quorum`) covers the terminal barrier |
| Lagging optional sink | holds completion | non-blocking `sink_snapshot_incomplete`, excluded from the resume position |
| Terminal | data-less boundary batch | first-class barrier, implemented by every sink |
| Bounds | retention bytes only (PostgreSQL) | connection semaphore, anchor age, plan storage, engine WAL and binlog checks |
| Ownership check | at control CASes and journal writes | before every publish |
| Legacy completion | `finished = true` | sink-checkpoint proof |

States: revision 1 had `allocated` (discovery resumable), then `running`, then `rows_produced` (could revert to `running`), then `completed`. Revision 2 has `allocated`, then `running`, then `rows_produced`, then `completed`, with **replacement by `g+1`** from any non-completed state whenever the read view is lost, and **no transition back**.

## 18. Questions for the reviewer

- **Q1. Resume position after completion** (section 6.3). Exclude lagging sinks from the minimum, with a `sink_snapshot_incomplete` incident? The alternative is to keep folding them in. That would pull the stream back to the anchor for every sink, which the replacement model cannot satisfy (no snapshot copy of a completed generation runs again).
- **Q2. S3 `legacy_rolling` barrier.** Close and upload the current file (proposed), or decline the barrier so that mode cannot complete a snapshot?
- **Q3. Default bounds** (section 9): anchor age 24 h, plan 256 MiB / 1,000,000 items, warning at 80%.
- **Q4. PostgreSQL completion mark.** Add `snapshot_completed: g` to the PostgreSQL completing checkpoint, as MySQL has, so legacy and current proof use one rule on both engines.
