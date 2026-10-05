# Durable Snapshot Queue - Design (revision 3)

**Status:** DRAFT for review. Design only: no production code, schema migration or test harness change accompanies this document.
**Date:** 2026-10-05
**Scope:** rc.1 item 3, after `snapshot-paging` (#128), gate reliability (#129) and the snapshot restart fixes (#131).
**Revision 2:** replaced the chunk-resume design of revision 1. A generation is bound to one database read view and is never resumed by another process.
**Revision 3:** adds the snapshot chain (cross-generation ordering), durable blocking for failures that require operator recovery, a frozen completion cohort, the process-wide connection cap, and the PostgreSQL completion mark. **Approved** as the implementation design (2026-10-05), with the clarification below.
**Clarification (approval condition):** legacy adoption is verified locally by each sink, through a per-sink adoption barrier (section 5.4); no sink comparator reads source control state. The connection cap is validated as ruled (section 9).
**Amendment 1 (2026-10-05):** the adoption barrier becomes a **per-generation start barrier**. Every generation, not only a chain's first over legacy state, starts with each frozen-cohort sink moving its own exact state to a generation-start position. One local mechanism now covers legacy entry, replacement of an incomplete generation, and a re-snapshot after CDC (sections 3.5, 4, 5.4, 10, 12-14).

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
13. **Revision 3 decisions:**
    - Lagging sinks are excluded after completion, with `sink_snapshot_incomplete`. Completion durably freezes the sink cohort, the policy parameters and the acknowledgements that justified it; later configuration never reinterprets an old terminal.
    - S3 `legacy_rolling` closes and uploads its current file at the barrier. For an empty snapshot it acknowledges without an object.
    - Bounds: 24 h; 256 MiB or 1,000,000 items, whichever is reached first; warning at 80%. The connection cap is process-wide.
    - PostgreSQL completing checkpoints carry `snapshot_completed: g`, bound to the exact anchor and continuity lineage. #131's unmarked structured completion at the exact anchor stays accepted; a bare LSN stays incomplete.
    - A durable random snapshot chain id orders a proven replacement `g < g+1`; different chains are incomparable.
    - Failures that require operator recovery stay halted across restarts until that recovery runs. Ordinary read-view or process loss replaces the generation automatically.
    - The terminal is bound to a policy snapshot. Configuration drift before completion explicitly replaces the generation.
14. **Approval conditions:**
    - Legacy ordering must be locally verifiable: a per-sink adoption barrier transitions each sink's exact legacy checkpoint or HEAD into the chain before any generation row.
    - The connection cap stays configuration-based. Aggregate oversubscription is allowed (a preflight warning, queued snapshots visible in status and metrics); per-source requirements are validated (section 9).
    - **Amendment 1:** before rows of every generation, each frozen-cohort sink compare-and-swaps its exact current state to `SnapshotGenerationAdopted { snapshot_chain, generation, replaced_digest }`. The previous state may be an approved legacy position, an older generation of the same chain, or a CDC position of the same lineage. Another chain, a foreign lineage or an unexpected state is refused. Adoption is resumed idempotently after a crash, no rows flow until the whole cohort adopted, and every replacement resets adoption to pending.
15. **Acceptance:**
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
- **Losing the view** (any of the above, or the process ending) **replaces the generation**: the next run allocates `g+1` in the same snapshot chain, plans and anchors again, and copies every table. Exceptions:
  - a generation whose terminal barrier its frozen policy frontier already covers is completed, not copied again (section 6);
  - a **blocked** generation is never replaced automatically. It stays halted, start after start, until an explicit proof-bound operator recovery (section 9.2).
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
  snapshot_chain     : ulid           # random; created with the source's first record, kept by every replacement
  legacy_through     : u64?           # pre-chain generations of this lineage adopted into the chain (section 10)
  generation         : u64
  lineage            : PersistedLineage
  fingerprint_format : 3              # configuration only; schema is bound per plan item
  config_fingerprint : hex
  state              : allocated | running | rows_produced | completed
  run                : ulid           # the process run that owns this generation (section 8)
  plan               : { sealed: bool, items: u64, bytes: u64, digest: hex }
  anchor             : EngineAnchor?  # set on entering running, never changed
  anchored_at_ms     : i64?           # wall time of the anchor (anchor-age bound)
  policy             : PolicySnapshot # frozen at allocation (section 6.2)
  terminal           : { digest: hex }?      # set on entering rows_produced; binds policy.digest
  completion         : { acks: [sink id], frontier: position }?   # set by the completed CAS
  blocked            : { reason: code, incident: id, since_ms: i64 }?  # section 9.2
  adoption           : none | pending | done   # this generation's start barrier (section 5.4); every allocation and replacement sets pending
  replaced           : u64?           # the generation this one replaced (reclamation)
```

`PolicySnapshot` is `{ mode: All | Required | Quorum, quorum: u32?, sinks: [{ id, required }], digest: hex }`, frozen from the pipeline configuration when the generation is allocated.

`EngineAnchor` is one of:
- `postgres { lsn, timeline?, chain?, transition? }` (the continuity stamp of #127 at the anchor, when proven)
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

**Snapshot positions in sink checkpoints:**
- incomplete: `{"snapshot":{"format":2,"snapshot_chain","generation","anchor"}}`. Format 1 (#131, no chain) stays readable as a legacy position.
- completing: an ordinary stream position at the anchor, marked `snapshot_completed: g` and `snapshot_chain` on both engines:
  - PostgreSQL: the mark is valid only with `lsn` equal to the anchor and the anchor's continuity stamp (timeline, chain, transition) when one was proven, compared exactly;
  - MySQL: as #131 (exact anchor position, same lineage).

  Older forms keep their #131 meaning: an unmarked structured PostgreSQL checkpoint exactly at the recorded anchor proves completion; a bare LSN is an incomplete position.

**Watermark.** For `durable_v2` sinks, O(1):

```text
WmPos::SnapshotSeq { snapshot_chain, generation, seq, completed }   # WATERMARK_VERSION 2
```

- `seq` is an in-memory publish counter: strictly increasing in publish order within one run, never persisted. A generation is published by one run only.
- The O(tables) `WmPos::Snapshot` vector is no longer produced; it remains readable only to classify legacy state.

### 3.5 Snapshot-to-CDC ordering (precise)

These rules apply only between positions of the same stable lineage; positions of different lineages are always incomparable.

**Notation:**
- `I(c, g, A)`: an incomplete position of chain `c`, generation `g`, anchor `A`;
- `C(c, g, A)`: the completing position;
- `S(p)`: a stream position at `p`;
- `W(c, g, s)`: a `SnapshotSeq` watermark;
- `L(g, A)`: a legacy position with no chain (#131 format 1, a bare LSN, the legacy vector watermark).

| a | b | order |
|---|---|---|
| `I(c, g, A)` | `I(c, g, A)` | Equal |
| `I(c, g, A)` | `I(c, g', A')`, `g < g'` | Before (a replacement within the chain is proven by its control record) |
| `I(c, g, A)` | `I(c, g, A')`, `A != A'` | Incomparable (one generation has one anchor) |
| `I(c, ...)` | `I(c', ...)`, `c != c'` | Incomparable |
| `I(c, g, A)` | `C(c, g, A)`, or `S(p)` with `p` strictly after `A` | Before |
| `I(c, g, A)` | `C(c, g', ...)` or a stream position of `g' > g` | Before |
| `I(c, g, A)` | `S(p)`, `p` at, before or incomparable with `A`, not attributable to a later generation | Incomparable |
| `C(c, g, A)` | `S(p)` | by stream order (`C` is the stream position `A`) |
| `C(c, g, A)` | `C(c, g', A')` | by `g`, then stream order |
| `W(c, g, s)` | `W(c, g, s')` | by `s` |
| `W(c, g, ...)` | `W(c, g', ...)`, `g < g'` | Before |
| `W(c, ...)` | `W(c', ...)` | Incomparable |
| `L(g, A)` | any chain `c` position | Incomparable. A legacy position enters a chain only through that sink's own start barrier (section 5.4) |
| `D(c, g)` (generation start: the sink entered generation `g` of chain `c`) | `D(c, g')` | by `g` |
| `D(c, g)` | `I`, `C` or `W` of chain `c`, generation `g' >= g` | Before |
| `D(c, g)` | `I`, `C` or `W` of chain `c`, generation `g' < g` | After |
| `D(c, ...)` | any position of another chain, a legacy position, or a stream position | Incomparable (a sink's own move from such a state to `D` is a local check, not an order; mixed sinks are classified, section 6.1) |

Every comparison above uses only the two positions themselves: a sink, including S3 `durable_v2` comparing its HEAD, never reads source control state. Per-sink classification (section 6.1) uses these rules.

**Mixed sinks during a replacement.** Sink X still holds `I(c, g, A)`, or an S3 `durable_v2` HEAD with `W(c, g, s)`, while sink Y has already committed `I(c, g+1, A')` or `W(c, g+1, s')`:
- the fold orders X before Y, so the resume classification sees X as behind;
- the next start replaces again within the chain, or completes if the frozen policy is covered;
- X's HEAD accepts `W(c, g+1, ...)` as later than `W(c, g, ...)`;
- nothing is incomparable and nothing blocks.

| Crash point during replacement | State left | Next start |
|---|---|---|
| before the replacement CAS | control at `g`, all sinks at `g` positions | replaces `g` (same decision again) |
| after the CAS, before any `g+1` publish | control at `g+1` `allocated`, sinks at `g` | `g+1` `allocated` is itself replaced by `g+2`; sinks at `g` order before both |
| after some sinks committed `g+1` positions | mixed `g` and `g+1` | as above; a `durable_v2` HEAD at `g+1` accepts `g+2` |

## 4. State machine

| From | To | Durable write | Condition |
|---|---|---|---|
| (none) | `allocated g` | control `slot_create` | lineage checked; no item key of `g` exists |
| `allocated g` | (same) | plan items `slot_create`, page by page | one discovery read view (PostgreSQL repeatable-read session; MySQL rediscovered under the lock at the anchor, #128) |
| `allocated g` | `running g` | control CAS: `plan` sealed, `anchor`, `anchored_at_ms`, `run` | discovery complete in this run; anchor taken; plan verified at the anchor |
| `running g` | `rows_produced g` | control CAS: `terminal` | every item read in this run's view; every worker joined successfully; every final check passed |
| `rows_produced g` | `completed g` | control CAS verifying `terminal` | the policy frontier covers the terminal barrier (6.2) |
| `allocated`, `running` or `rows_produced` `g`, not blocked | `allocated g+1` (`replaced = g`, same chain) | control CAS from the read version | process start finds `g` not `completed` and not coverable; or this run lost its read view; or the policy snapshot no longer matches the configuration (section 6.2) |
| any non-completed `g` | (same) with `blocked` set | control CAS | a failure that requires operator recovery (section 9.2) |
| blocked `g` | `allocated g+1` | control CAS by the recovery CLI | operator `resnapshot` with its proof |
| `completed g` | `allocated g+1` | control CAS | operator `resnapshot`, or mode `always` |

Rules:
- No row of `g` is published unless the control record is `running`, `g` is current and `run` is this run.
- **A start in `rows_produced`** first computes the frozen policy frontier:
  - frontier covers the terminal: CAS to `completed`, no copy;
  - otherwise: replace with `g+1`.
- **A start in `allocated` or `running`** replaces with `g+1`, unless blocked.
- **No row of a generation is published while its `adoption` is `pending`** (section 5.4). Allocation and every replacement set it to `pending`.
- **A start in a blocked generation** re-raises its incident and stops before any row or replacement. Only the recovery CLI clears it.
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
| S3 `legacy_rolling` | close and upload the current file, then acknowledge (still the weaker durability that mode declares); with nothing buffered (an empty snapshot) acknowledge without an object, the checkpoint store recording the boundary |

The trait default refuses (`SinkError::Unsupported`). A sink that does not implement the barrier therefore cannot complete a snapshot: the policy decides whether that blocks completion. It is never silently treated as acknowledged.

### 5.3 Use (terminal)

- After `rows_produced`, the publisher sends one barrier with the completing position and `WmPos::SnapshotSeq { completed: true }`.
- This replaces #131's held-back final event. The all-empty snapshot completes through the same barrier, which removes #131's empty-snapshot recopy residual.

### 5.4 Generation start barrier (amendment 1)

Every generation starts with each sink of its frozen cohort moving its own durable state into that generation, before any row of it is published. This covers a chain's first generation over legacy state, the replacement of an incomplete generation, and a re-snapshot after a completed generation's CDC (mode `always`, operator `resnapshot`). Each sink verifies the transition locally, through a barrier of kind `Start { snapshot_chain, generation, lineage, legacy_through }`:

1. After the anchor and before the first publish of a generation whose control record has `adoption = pending`, the source sends the start barrier.
2. Each sink checks its **own** durable state and acts:
   - **Accepted previous states.** Its state is one of these:
     - empty;
     - a legacy (chain-less) position of `lineage` with generation at most `legacy_through`;
     - any position of chain `c` with a generation below `generation` (incomplete, completing, sequence watermark or start);
     - a CDC position of `lineage`.

     The sink compare-and-swaps it, from exactly the state read, to `D(c, generation)`, recording the digest of the replaced state. S3 `durable_v2` does this with a HEAD CAS whose expected value is the exact HEAD it read.
   - **Already in the generation.** `D(c, generation)` or any other position of generation `generation` of chain `c`: acknowledge (idempotent).
   - **Anything else** refuses, including a position of a **later** generation of chain `c` (the caller's control state is stale, rewound or corrupt; acknowledging it could let `generation` publish while the sink is already past it), another chain, another lineage, or an unknown format.

   | Stored state | Decision |
   |---|---|
   | empty | move |
   | chain `c`, generation below `generation` | move |
   | chain `c`, generation equal to `generation` | already (acknowledge) |
   | chain `c`, generation above `generation` | refuse |
   | approved legacy (up to `legacy_through`) or CDC of `lineage` | move |
   | another chain, another lineage, unknown format | refuse |
3. The delivery task commits `D(c, generation)` to the per-sink key of every sink that acknowledged.
4. When **every** sink of the frozen cohort has acknowledged (not policy-weighted: a sink left behind could never accept the generation), the source sets `adoption = done` by control CAS. A refusal fails the run with `snapshot_state_invalid`, class `adoption_refused`, which blocks.

**Empty state.** It is accepted for any generation, not only a chain's first (approved). A sink added to the cohort later starts empty in a later generation, and the policy-change replacement (section 6.2) would otherwise block on it forever. It has no prior checkpoint to preserve, and the full replacement snapshot supplies its baseline. External side effects it made without a checkpoint may be duplicated, which is within the at-least-once contract.

**Comparators.**
- A comparator never orders a legacy or CDC position against `D`: entering a generation is the sink's own checked transition, not an order.
- After the transition, `D(c, g)` orders before every position of generation `g` and later in chain `c`, so the generation's first publish is accepted.
- Different chains stay incomparable.

| Crash point | State left | Next start |
|---|---|---|
| after the allocation or replacement CAS (`adoption = pending`), before the barrier | every sink in its previous state | the generation is replaced (pending again); its start barrier runs before any row |
| after some sinks adopted | mixed previous states and `D(c, g)` | the generation is replaced by `g+1`: sinks at `D(c, g)` move to `D(c, g+1)` (an older generation of the chain), the others move from their previous state |
| after every sink adopted, before `adoption = done` | all `D(c, g)` | as above (every move is accepted) |
| after `done` | ordinary generation | ordinary chain ordering (section 3.5) |

## 6. Policy frontier, completion and resume

### 6.1 Per-sink classification

Every per-sink checkpoint of the source is classified against the control record's current generation `g` and anchor `A`, as one of:
- **behind:** an `I(g, A)`, or a position of an older generation, or none;
- **at-or-past terminal:** `C(g, A)` or a stream position strictly after `A`;
- **foreign:** anything incomparable (another lineage, generation or anchor of the current lineage that is not older, an unknown format).

Foreign fails closed (`snapshot_state_invalid`).

### 6.2 Frozen policy and frontier

**Freezing.** When `g` is allocated, the control record freezes the pipeline's commit policy as a `PolicySnapshot`:
- the mode;
- the quorum, if any;
- every sink id with its `required` flag;
- a digest over them.

The terminal digest binds it: `sha256(snapshot_chain, g, plan.digest, policy.digest)`.

**Drift.** At every start and before the `completed` CAS, the current configuration's policy digest is compared with the frozen one:
- **Before completion, a difference** (a sink added, removed, made required or optional, a changed mode or quorum) **explicitly replaces the generation**. It raises a non-blocking `snapshot_replaced` incident with class `policy_changed`, because the terminal of one cohort must not be judged by another.
- **After completion, configuration changes never reinterpret the terminal.** The `completion` record keeps the acknowledging sink ids and the frontier. A sink added later is outside that cohort: it is behind, gets `sink_snapshot_incomplete`, and needs a re-bootstrap.

**Coverage.** Evaluated only over the frozen cohort, the frontier covers the terminal when:

| Policy | Covered when |
|---|---|
| `All` | every sink is at or past the terminal |
| `Required` | every `required: true` sink is |
| `Quorum(q)` | at least `q` sinks are |

The `rows_produced` to `completed` CAS records `completion = { acks, frontier }`: the cohort sinks whose checkpoints were at or past the terminal, and the frontier position that satisfied the policy.

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
| Snapshot connections | source DBs, whole process | process-wide `runtime.max_snapshot_connections` (default 64), one semaphore shared by every pipeline in the process; per source, `snapshot.max_snapshot_connections` (default `max_parallel_tables x max_parallel_chunks + 2`) caps its share | none (it queues) | never exceeded |
| Anchor age | source DB view held open, source log retained | `snapshot.max_anchor_age` (default 24 h) | 80%: `snapshot_bound_warning` (non-blocking) | stop, replace nothing automatically, `snapshot_anchor_unavailable` |
| Plan storage | DeltaForge store | `snapshot.max_plan_bytes` (default 256 MiB) and `snapshot.max_plan_items` (default 1,000,000), checked during discovery | 80%: warning | discovery stops before sealing; the generation fails with `snapshot_bound_exceeded` |
| PostgreSQL WAL retention | source WAL | from the slot: `safe_wal_size` and `wal_status` of the snapshot's slot | `safe_wal_size` below 20% of `max_slot_wal_keep_size`, or `wal_status = unreserved`: warning | `wal_status = lost`, or `safe_wal_size <= 0`: stop with `snapshot_anchor_unavailable` |
| MySQL binlog retention | source binlog | the existing purge guard: the anchor's file and GTID set still on the server; retention age against the anchor age | anchor age above 80% of `binlog_expire_logs_seconds`: warning | anchor file purged or GTID set no longer covered: stop with `snapshot_anchor_unavailable` |

- **Connection acquisition never deadlocks.**
  - A snapshot acquires its base permits (coordinator, plus the lock connection on MySQL, plus one worker) atomically, as one acquisition of that many permits. The acquisition is cancellable, and happens before any catalog transaction, lock or snapshot anchor is opened.
  - Workers above the first, and intra-table readers, take one permit each and release it when done.
  - No partial base is retained while waiting.
- **Validation** (configuration-based):
  - the global cap is positive;
  - each source's base requirement fits its per-source cap;
  - each source's base and maximum request fit within the global cap.

  A violation fails that source's configuration check.
- **Oversubscription is allowed:** if the configured sources' aggregate demand exceeds the global cap, startup emits a preflight warning and proceeds. Admission control queues snapshots. Queued snapshots are visible in pipeline status and in metrics (`deltaforge_snapshot_queued{pipeline}`, `deltaforge_snapshot_connection_permits{state}`).
- Queue storage (plan) and source-log retention are separate resources, with separate names and incidents.

### 9.2 Blocking: failures that require operator recovery

These failures set `blocked` on the control record (by CAS, with the reason and incident id) and stop:

| Reason | Cause |
|---|---|
| `snapshot_anchor_unavailable` | retention lost or about to be lost (PostgreSQL WAL, MySQL binlog), anchor-age hard bound |
| `snapshot_bound_exceeded` | plan storage hard bound |
| `snapshot_state_invalid` | corruption, unknown format, version-reuse probe, concurrent owner |

- **A blocked generation stays halted across restarts:** every start re-raises the incident and stops before any row or replacement.
- **Recovery** is the recovery CLI's proof-bound `resnapshot`, which replaces it by `g+1` (clearing `blocked`), with actor and reason audited.
- **If the `blocked` CAS itself fails** (a store failure, a crash before it), the next start re-runs the detection: the retention, anchor-age and storage checks run at every start before any decision, and corruption is found again on read. It sets `blocked` then.
- **Ordinary loss is not blocking:** a process restart, a lost read view or a policy change replaces the generation automatically.

## 10. Legacy state and unknown formats

| Stored | Classification | Action |
|---|---|---|
| Control `record_format` below 3 (any status) **and** the policy frontier covers a completion proven by sink checkpoints | completed legacy | lineage verified; CAS in place to `record_format 3`, `completed`, in a new chain with `legacy_through = g` (a later generation of the chain starts through its start barrier, which accepts these legacy positions); the completion records the current policy and acknowledging sinks; legacy progress deleted after |
| Control below 3, anything else | incomplete legacy | lineage verified; CAS to `g+1` `allocated` in a new chain with `legacy_through = g` and `adoption = pending`; every sink moves its own legacy position into the generation through the start barrier (section 5.4) before any row; one-time full recopy |
| Unknown `record_format`, `fingerprint_format`, `item_format`, snapshot-position `format` or watermark version | unknown | refused; record untouched; incident before any row |
| Other stable lineage | foreign | refused (`ConfigChanged`); record untouched |

**Proof of a legacy completion comes from the sink checkpoints only**, using the #131 rules:
- PostgreSQL: an unmarked structured stream position exactly at the legacy progress anchor (`start_lsn`), or strictly after it, for every sink the policy requires, where the anchor is readable; a bare LSN or an incomplete position is not proof.
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
| Block | control CAS setting `blocked` | the next start detects the condition again and blocks | halted until recovery |
| Generation start | per-sink CAS to `D(c, g)`, then control `adoption = done` | (section 5.4 table) | (section 5.4 table) |
| Policy drift | control CAS replacing `g` | replaced at the next start | `g+1` with the new policy |

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
- **I15** Every snapshot connection is taken from the process-wide semaphore. A source's share never exceeds its `snapshot.max_snapshot_connections`, and base permits are acquired atomically.

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
- **I13** Each bound's hard limit stops the run before the resource is lost, with its incident, and blocks the generation.
- **I16** A blocked generation publishes no row and is not replaced, on any start, until the recovery CLI's `resnapshot`.
- **I17** Within one snapshot chain, every position of `g` orders before every position of `g' > g`. Positions of different chains are incomparable, except legacy positions the chain adopted.
- **I18** `completed` is decided only over the frozen cohort with the frozen policy. A configuration difference before completion replaces the generation, and after completion changes nothing about it.
- **I19** At no time does the process hold more snapshot connections than `runtime.max_snapshot_connections`.
- **I20** No sink comparator reads source control state. A legacy or CDC position is never ordered against a chain's generation start, but enters a generation only through that sink's own checked compare-and-swap. No row of a generation is published while its start barrier is pending, and every allocation and replacement resets it to pending.

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
- legacy completed with proof, legacy without proof, unknown format;
- generation start after CDC: generation `g` completes, the S3 HEAD and the sink checkpoints move to CDC positions, then mode `always` or `resnapshot` allocates `g+1`, whose start barrier moves both and whose rows are accepted;
- a partial start barrier crash across S3 `durable_v2` and an ordinary sink, then the replacement's barrier completes it;
- another chain's state, and CDC state of a foreign lineage, still refused by the start barrier;
- legacy entry against durable S3 (MinIO), starting from a genuinely legacy HEAD written by the pre-queue release format:
  - adopt it, with a crash at each adoption and replacement boundary of section 5.4;
  - then accept `g+1`;
  - assert that a HEAD of another chain and an unadopted legacy HEAD are still refused;
- replacement with mixed sinks: crash after the replacement CAS and after a partial `g+1` commit, against durable S3 (MinIO HEAD watermarks in two generations of one chain) and an ordinary sink; then completion;
- each blocking reason, across two restarts (still halted), then the recovery `resnapshot`;
- policy drift before completion (replacement) and after (no reinterpretation, `sink_snapshot_incomplete` for the new sink);
- two pipelines sharing the process-wide connection cap without deadlock: queued status and metrics; cancellation while queued; no partial base held while waiting; per-source validation failures.

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
   - control record format 3 (snapshot chain, frozen policy, completion, blocked) and plan items in the `snapshot_plan` namespace;
   - classification, allocation, replacement and reclamation;
   - snapshot position format 2 and the `WmPos::SnapshotSeq` watermark, with the chain-aware comparators;
   - the shared queue contract suite on the three backends.
2. **Barrier:**
   - `SourceItem::Barrier` (terminal and generation-start kinds), the delivery-task operation and `Sink::barrier`;
   - each sink's implementation and test, including the start CAS on S3 `durable_v2`.
3. **PostgreSQL wiring:**
   - paged plan;
   - generation-scoped view and worker retry;
   - barrier completion;
   - the process-wide connection semaphore;
   - the WAL and anchor-age bounds.
4. **MySQL wiring:**
   - the same;
   - the binlog bound;
   - worker-loss replacement;
   - keyless tables chunked by a bounded scan.
5. **Completion and resume:**
   - per-sink classification, the frozen policy frontier and drift replacement;
   - blocking and its start-time checks;
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
| Completion | per-sink minimum over all sinks covers the terminal | the policy frontier, over a cohort frozen at allocation, covers the terminal barrier; acknowledgements recorded |
| Cross-generation ordering | incomparable | ordered within one snapshot chain (`g < g+1`); chains incomparable; legacy adopted explicitly |
| Retention, corruption, hard bounds | stop; the next start replaced the generation | `blocked`: halted across restarts until the recovery CLI |
| Connection cap | per source | process-wide semaphore, atomic base permits |
| Lagging optional sink | holds completion | non-blocking `sink_snapshot_incomplete`, excluded from the resume position |
| Terminal | data-less boundary batch | first-class barrier, implemented by every sink |
| Bounds | retention bytes only (PostgreSQL) | connection semaphore, anchor age, plan storage, engine WAL and binlog checks |
| Ownership check | at control CASes and journal writes | before every publish |
| Legacy completion | `finished = true` | sink-checkpoint proof |

States:
- **Revision 1:** `allocated` (discovery resumable), then `running`, then `rows_produced` (could revert to `running`), then `completed`.
- **Revision 3:** `allocated`, then `running`, then `rows_produced`, then `completed`, with:
  - **replacement by `g+1` in the same chain** from any non-completed, non-blocked state when the read view is lost or the policy changed;
  - **`blocked`** (orthogonal, on any non-completed state) for failures that require operator recovery, cleared only by `resnapshot`;
  - **no transition back.**

## 18. After rc.1

A separate scale-qualification milestone, for the real topology: about 300 sources, 1,700 to 2,000 schemas, and up to 400K tables per source.
