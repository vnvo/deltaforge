# Durable Snapshot Queue - Design

**Status:** DRAFT for review. Design only: no production code, schema migration or test harness change accompanies this document.
**Date:** 2026-10-05
**Scope:** rc.1 item 3 (`snapshot-durable-queue`), after `snapshot-paging` (#128) and gate reliability (#129).
**Basis:** review rulings of 2026-10-05 (recorded in section 0), the rc.1 milestone plan (durable snapshot queue item), and the code as of `ece9e20`.

## 0. Rulings this design implements

1. One implementation PR, both engines, separately reviewable commits; one core gate on the final accepted tree.
2. Incomplete legacy generations restart as the next generation: explicit format detection, stable lineage verified, CAS N to N+1, work reset before any row, crash before the CAS retries, crash after it resumes the new generation, no old progress or event identity mixed in, unknown future formats refused, the one-time recopy documented. A demonstrably completed legacy snapshot is never restarted automatically.
3. Generation states `Allocated`, `Running`, `RowsProduced`, `Completed`. `RowsProduced`: the terminal boundary exists durably. `Completed`: that boundary passed the pipeline's required sink-commit policy and the durable resume checkpoint covers it. The terminal boundary identity is stored in the generation control record; completion is a CAS that verifies it; a `Completed` generation never copies rows again.
4. Acceptance on the reference machine: progress plus frontier bytes 10K/1K at most 12x; wall time 10K/1K at most 15x; resident queue/work state bounded by page/chunk size and worker count; durable operation counts with a structurally linear formula; absolute times and bytes reported.
5. Backends: in-memory for semantics and concurrency; SQLite for crash/reopen and corruption; PostgreSQL for CAS, ordering, restart and partial failure; one full interrupted snapshot/resume per engine on a real durable backend.

## 1. What exists today (facts)

**Progress.** One JSON record per source (`snapshot_progress:{id}` for PostgreSQL, `mysql_snapshot_progress:{id}` for MySQL) holding the anchor, `done_tables` (the full set) and `finished`. It is rewritten in full after every completed table, best effort (write errors ignored), and written when a table has been *read*, not when its rows were acknowledged by any sink.

**Resume.** Neither engine resumes in production:
- PostgreSQL re-anchors an owned inactive slot (drop and recreate) and resets progress: a full re-snapshot (`postgres_slot_owner.rs:402-417`).
- MySQL restarts an interrupted snapshot as a new generation (`mysql/mod.rs:629-653`, the F2 fix).
- `docs/src/sources/mysql.md:183-184` and `docs/src/sources/postgres.md:254-260` still claim table-granularity resume. They are wrong and are corrected by this PR's documentation commit.

**Frontier and boundaries.** `SnapshotAggregator::current_boundary` serializes every table's cursor into one `WmPos::Snapshot` watermark (O(tables) bytes) roughly once per chunk and once per table. 10,000 MySQL tables: 20,001 boundaries, 9,614.7 MiB of watermarks; 10,002 progress writes, 1,236.9 MiB ([capacity envelope](../src/capacity-envelope.md)).

**Generation record.** `snapshot_generation:{id}` (CAS, `SnapshotStateStore`): generation, lineage, status, config fingerprint (format 2, binding every table's schema version), fingerprint format. `update_status` has no production caller: no generation ever leaves `Allocated`.

**Delivery and checkpoints.**
- One FIFO delivery task per pipeline; batches close at boundaries; the commit policy (`All`, `Required` default, `Quorum`) is evaluated before any commit; succeeded sinks then commit `{source}::sink::{sink}`.
- The checkpoint committed is the last *event's* boundary checkpoint (`coordinator.rs:2417`): a data-less `SourceItem::Boundary` closes a batch but commits nothing.
- The resume checkpoint is the minimum over all per-sink keys (required or not), failing closed on any incomparable pair (`pipeline_manager.rs:120-177`).

**Finding F-1 (likely existing defect, to confirm).** The PostgreSQL snapshot boundary checkpoint is the anchor LSN as text (`postgres_snapshot.rs:323`, `"X/Y"`), not `PostgresCheckpoint` JSON. Snapshot chunks carry it on their last event, so a sink commit during a PostgreSQL snapshot stores it per sink. `compare_pg_checkpoints` parses JSON only, and the per-sink fold validates even a lone checkpoint by self-comparison, so a restart after any snapshot commit and before the first CDC commit should fail closed with an incomparable-checkpoint error. Fail-closed (no loss), but it blocks restarts. See question Q1.

**Storage primitives** (`crates/storage`).
- Slots: per-key versions from 1, `slot_create` (create-only), `slot_cas`, `slot_list` (byte order, exclusive cursor, page at most 4,096).
- Logs: `log_append_if_absent` idempotent by capture id; global seq, gap-prone and not commit-ordered on PostgreSQL.
- **No multi-key atomicity.** Deleting and recreating a slot restarts its version at 1 (ABA).
- SQLite runs WAL with `synchronous=NORMAL`: a power loss can drop the most recent commits, never reorder them.

**Ownership.** There is no per-pipeline lease. A pipeline claims its source id with a create-only slot inside one process; two processes against one source are not prevented ([deployment support](../src/deployment-support.md)).

## 2. Goals and non-goals

**Goals:**
- Resume an interrupted snapshot at chunk granularity on both engines, without loss, under the existing at-least-once contract.
- O(1) bytes per boundary and per progress write.
- Structurally linear durable work.
- Resident state independent of the table count.
- An authoritative, sink-acknowledged generation completion.

**Non-goals:**
- Exactly-once delivery.
- Leases or fencing between processes (detection only, section 5.4).
- Intra-chunk resume.
- Changing the snapshot row identity scheme.
- The recovery CLI (its `resnapshot` and `diagnose` build on the records defined here).

## 3. Terms

- **Generation `g`.** One complete initial load: a plan, an anchor, chunks, a terminal boundary.
- **Anchor `A`.** The CDC start position of `g`: the PostgreSQL slot's consistent point, or the MySQL binlog/GTID position captured under `FLUSH TABLES WITH READ LOCK`.
- **Item.** One planned table of `g`.
- **Chunk.** A half-open cursor range of one item, read and published as a unit.
- **Publish sequence `s`.** A per-generation counter, strictly increasing in publish order, assigned to every chunk and to the terminal boundary.
- **Acknowledged.** A chunk with `s <= M`, where `M` is the minimum per-sink snapshot position of `g` (the resume checkpoint).

## 4. Durable record formats

All records are JSON with an explicit format field.
- Any format the binary does not know is refused, never interpreted (fail closed, record untouched).
- Generation numbers are never reused, and every per-generation key embeds the generation, so no key is ever deleted and recreated under the same name (no ABA).
- Keys escape the source id as the incidents adapter does (`segment()`).
- Hex below means lowercase fixed-width hex.

### 4.1 Generation control record

The existing record `snapshot_generation:{source}`, upgraded in place (`SnapshotStateStore` CAS). This keeps continuity with the #128 rules: lineage check, `classify_format`, CAS replacement.

```text
GenerationControl (record_format 3)
  record_format     : 3            # absent = 1 (pre-format), 2 = #128 record
  generation        : u64
  lineage           : PersistedLineage
  fingerprint_format: 3            # config fingerprint, schema versions no longer bound (5.3)
  config_fingerprint: hex sha256
  state             : allocated | running | rows_produced | completed
  run               : ulid         # the process run that last wrote the record (5.4)
  anchor            : EngineAnchor?        # set on entering running; never changes
  plan              : { sealed: bool, items: u64, discovery_after: key?, digest: hex }
  next_seq          : u64          # lowest publish sequence never handed out (a hint, see 6.2)
  folded_through    : u64          # every chunk with s <= this is folded into its item (6.4)
  terminal          : { seq: u64, digest: hex }?   # set by rows_produced
  replaced          : u64?         # the generation this one replaced, for reclamation (8.3)
```

`EngineAnchor` is one of:
- `postgres { lsn, slot, timeline, chain, transition }`
- `mysql { file, pos, gtid_set, server_uuid }`

The record stays O(1) in the table count.

### 4.2 Work items

Namespace `snapshot_queue`, key `{source}/{g:016x}/i/{hex(qualifier)}/{hex(table)}`. Hex of the raw bytes, separated by `/` (which sorts below every hex digit), so key order is the discovery order `(qualifier, table)` bytewise.

```text
WorkItem (item_format 1)
  qualifier, table      : string
  identity              : [IdentitySpec]           # PostgreSQL kinds / MySQL columns
  cursor_kind           : signed | unsigned | ctid_block
  schema_version        : u64                      # registry version the plan was made from
  signature             : hex                      # whole registered schema model (#128)
  acked                 : SnapshotCursor           # every row below it is acknowledged
  pending               : [(start, end)]           # acknowledged ranges above `acked`, at most max_parallel_chunks
  done                  : bool                     # acknowledged to the end of the table
```

- Items are created once by `slot_create` during discovery.
- Items are updated only by the fold (6.4), with a CAS on the item's own version.
- An item is O(identity columns + `max_parallel_chunks`) bytes.

### 4.3 Chunk journal (the delta frontier)

Namespace `snapshot_queue`, key `{source}/{g:016x}/c/{s:016x}`, written with `slot_create`, so a retry with identical bytes is idempotent and a different value at the same sequence is a detected conflict.

```text
ChunkEntry (chunk_format 1)
  seq     : u64
  item    : item key suffix
  range   : (start, end)            # half-open
  last    : bool                    # the item's final chunk
  rows    : u64
```

The journal is the only per-chunk durable write. Entries are deleted once folded (6.4).

### 4.4 Snapshot positions in sink checkpoints

Sink checkpoints keep the engine's checkpoint type with one new shape for snapshot positions:

```text
{ "snapshot": { "format": 1, "generation": g, "seq": s,
                "terminal": digest?, "anchor": EngineAnchor } }
```

The object deliberately has **no top-level `lsn`** (PostgreSQL) or `file`/`pos` (MySQL). A pre-queue binary therefore cannot parse it as a stream position and fails closed instead of resuming CDC at the anchor with the snapshot unfinished (section 10).

The durable watermark used by `durable_v2` sinks gains one variant, also O(1):

```text
WmPos::SnapshotSeq { generation, seq, terminal: digest? }    # WATERMARK_VERSION 2
```

The O(tables) `WmPos::Snapshot` vector is no longer produced. It remains readable only to classify legacy state (section 9).

**Ordering**, extending `compare_*_checkpoints` and `compare_watermarks`, same lineage only:
- `(g, s) < (g, s')` iff `s < s'`.
- For any `s`, `(g, s)` sorts before any CDC position of `g`'s lineage at or after `A`.
- A terminal position `(g, s_T, digest)` equals another only with the same digest; a different digest at the same sequence is incomparable.
- Positions of different generations are incomparable, except that a position of a generation lower than the control record's current generation is treated as "no progress in the current generation" (8.2).

## 5. Ordering, ownership and binding

### 5.1 Ordering

- Discovery order is `(qualifier, table)` bytewise, as in #128.
- Workers take items in key order from a paged cursor over the item keys (page = `discovery_page_size`).
- Publish order is sequence order: the publisher assigns `s` and sends to the pipeline under one lock, and the delivery task is FIFO. So a sink commit of snapshot position `(g, s)` implies that sink received every chunk with sequence at most `s`.

### 5.2 Binding

- Every item, journal entry and checkpoint carries `g`, and `g` binds the lineage and the anchor through the control record.
- Positions are only compared within one lineage (existing rule).
- Schema: an item carries the registry version and the #128 whole-model signature it was planned from.
- On resume, the anchor check of #128 (`verify_plan_*`) runs over the *pending* items only, in pages.

### 5.3 Fingerprint

- `fingerprint_format 3` binds the configuration only: engine, table patterns, identity configuration, chunking and cursor configuration.
- It no longer binds each table's schema version. Schema is bound per item, so a schema change of one pending table is handled per item (Q3) instead of invalidating the whole generation.
- `classify_format` gains 3 as current. A format 2 record is a legacy record (section 9).

### 5.4 Ownership

Single ownership per source remains a deployment precondition. The queue detects violations where it can, and fails closed:
- Each process run writes a new `run` into the control record (CAS) before its first queue write.
- Every later control CAS checks that `run` is unchanged; a change means another process took over, and the run stops with an incident.
- Journal entries are create-only: two writers at one sequence collide (`slot_create` returns no version).

This is detection, not a lease (Q8).

## 6. Progress, acknowledgement and the frontier

### 6.1 Reading and publishing a chunk

1. A worker reads the chunk's rows in the read view of the current run (6.6).
2. Under the publisher lock it takes `s = next++` and writes `ChunkEntry(s)` (`slot_create`).
3. It then sends the chunk's events, the last carrying the boundary checkpoint `(g, s)`.
4. A failed journal write fails the run before any event of that chunk leaves the publisher.

### 6.2 The resume point

- `M` is the per-sink minimum snapshot position of `g`, read through the existing per-sink proxy. Absent means nothing is acknowledged.
- Journal entries with `s <= M` are acknowledged; entries with `s > M` are not, and their chunks are read again under new sequences.
- On start, `next` is one past the highest journal key (a reverse probe of one page), never lower than the control's `next_seq` hint.

### 6.3 What is acknowledged

A chunk is acknowledged exactly when some boundary `(g, s' >= s)` was committed by every sink, which is what `M` expresses. No source-side write claims acknowledgement: the only source of truth for it is the sink checkpoints. The per-sink fold (minimum over all sinks, required or not) is unchanged.

### 6.4 Fold and reclamation

Periodically (Q9: every 256 acknowledged entries or 5 s), and once at startup:
1. Read `M`.
2. Page through journal entries in `(folded_through, M]`.
3. For each affected item, merge the ranges into `acked`/`pending`/`done` with a CAS on the item. The merge is idempotent: a range already covered changes nothing.
4. CAS the control record's `folded_through` to the highest folded sequence (and refresh `next_seq`).
5. Delete the folded journal entries.

A gap in `(folded_through, M]` (a sequence acknowledged by sinks with no journal entry) is corruption: fail closed (section 11).

### 6.5 Frontier

- **Durable frontier:** the items (`acked` plus at most `max_parallel_chunks` pending ranges each) plus the unfolded journal window.
- **Per boundary:** one O(1) checkpoint.
- **Resident:** only the in-flight chunks and the items currently being read.

### 6.6 Read view on resume (the correctness argument)

The first run reads every chunk in the anchor's read view. A resumed run reads the remaining chunks in a new read view taken at its own start (`T2`, after `A`):
- PostgreSQL: a new exported snapshot;
- MySQL: new `START TRANSACTION WITH CONSISTENT SNAPSHOT` workers, with no lock needed.

CDC still starts at `A`. Under the at-least-once contract this loses nothing:
- every committed change after `A` is replayed by CDC;
- every row present at its chunk's read time is copied;
- a row deleted between `A` and its read time is not copied, and its delete is replayed.

Current-state sinks converge once CDC passes `T2`. Append-only sinks may see the row twice: a chunk read again keeps the same snapshot event identity (`snapshot_row_event_id` is per generation, table and row identity), and a CDC event has its own identity.

This widens the accepted PG-A-lite overlap from `(A, T_snapshot]` to `(A, T_last_resume]` (Q2).

**Precondition for any resume:** the anchor must still be retained.
- PostgreSQL: the slot-bounds proof of #123 with `F = A` (`restart_lsn <= A`, `confirmed_flush_lsn <= A`), and the continuity chain of `A`.
- MySQL: the GTID set (or file) of `A` not purged.

Otherwise the generation cannot continue (section 11).

## 7. Generation state machine

| From | To | Write | Condition |
|---|---|---|---|
| (none) / replaced | `allocated` | CAS (expect absent, or the replaced record's version) | lineage checked; no item key of the new generation exists (`slot_list` prefix, limit 1) |
| `allocated` | `allocated` | CAS (`plan.discovery_after`, `plan.items`) | after each durable discovery page |
| `allocated` | `running` | CAS (`anchor`, `plan.sealed`, `plan.digest`) | discovery complete; anchor taken (PostgreSQL slot; MySQL position read under the lock) |
| `running` | `rows_produced` | CAS (`terminal = {s_T, digest}`) | every item read to its end (acknowledged in an earlier run or read in this one) and every chunk journaled; `digest = sha256(g, s_T, plan.digest, plan.items)` |
| `rows_produced` | `completed` | CAS verifying `terminal` | `M` is the terminal position with the same digest, or a CDC position of the lineage at or after `A` |
| `rows_produced` | `running` | CAS clearing `terminal` | at startup when `M` is below the terminal (6.2): the terminal was never acknowledged by all sinks |
| any | (replaced by `g+1`) | CAS on the control record | legacy restart (9), unrecoverable anchor or operator `resnapshot` (11) |

Rules:
- No row of `g` is published before the control record is `running` with its anchor.
- `completed` refuses all row copying. A start in `completed` streams CDC from the resume checkpoint `M`.
- CDC starts right after the terminal boundary is published (in `rows_produced`). The `completed` CAS follows asynchronously, when the source observes the per-sink minimum reaching the terminal (the existing checkpoint-change notification), or at the next start (reconciliation).

**Discovery.** Items are written page by page while `allocated`. An interrupted discovery continues from `plan.discovery_after`. Only a sealed plan defines the table set, so an interrupted discovery never marks a table absent.

## 8. Terminal boundary and required sinks

### 8.1 Steps (no single transaction spans the control record and the sink checkpoints)

1. When the last item's last chunk is journaled, CAS `running` to `rows_produced` with `terminal`.
2. Publish the terminal boundary `(g, s_T, digest)`.
   - It is data-less when the last chunk's events were already sent, so **the delivery task must commit the checkpoint of a data-less boundary batch**. Today only event-borne checkpoints are committed. The batch reaches every sink as an empty batch with the boundary context, and the commit policy is evaluated as for any batch (Q7).
3. The sinks acknowledge it, and the delivery task commits it per sink under the commit policy.
4. When the per-sink minimum covers the terminal, CAS `completed` (verifying `terminal`).
5. At startup in `rows_produced`, reconcile:
   - `M` covers the terminal: CAS `completed`;
   - otherwise: revert to `running` (7) and resume.

   An unacknowledged terminal is never trusted.

### 8.2 Partial per-sink commits

| Situation at restart | Outcome |
|---|---|
| Every sink at or past the terminal (snapshot terminal or CDC) | `completed`; CDC from `M` |
| Required sinks past the terminal, an optional sink behind | `M` is the optional sink's position: the generation stays `rows_produced`, reverts to `running`, and the chunks above `M` are read again (duplicates for the sinks that had them). Same semantics as today: a lagging sink lowers the resume point. |
| A required sink committed, another failed | the policy failed the batch and the pipeline; `M` is below the terminal; revert and resume |
| A sink checkpoint of an older generation | treated as no progress in the current generation (the current generation was allocated by CAS after deciding to replace it); the current generation runs from its start |
| A sink checkpoint of a newer generation, or a mismatched terminal digest | corruption: fail closed |
| `durable_v2` S3 HEAD ahead of its per-sink key (crash between the two) | `M` is lower: re-delivery, duplicates only |

A permanently failing optional sink therefore holds the generation in `rows_produced` and retains the anchor (Q4).

### 8.3 Reclamation of generations

- After `completed`: the generation's items and any journal remainder are deleted in pages.
- After a replacement (`replaced = g-1`): the replaced generation's keys are deleted the same way.
- Deletion is idempotent and never touches the current generation's prefix.
- The legacy progress records are deleted only after the replacing generation's control CAS (9).

## 9. Legacy state and unknown formats

Legacy state is classified by format only:

| Stored | Classification | Action |
|---|---|---|
| Control `record_format` absent/1/2, legacy progress `finished = true`, per-sink minimum a CDC position or a completed legacy snapshot position | demonstrably completed | lineage verified; CAS in place to `record_format 3`, `state completed` (no items, no terminal); never restarted |
| Control `record_format` absent/1/2, anything else (unfinished, or no progress, or snapshot positions not completed) | incomplete legacy | lineage verified; CAS to `g+1` `allocated` (`replaced = g`); then delete legacy progress and legacy snapshot per-sink positions are ignored (8.2); full run of `g+1` |
| A sink checkpoint that is a bare PostgreSQL LSN text (F-1) | legacy PostgreSQL snapshot position | completed only together with legacy `finished = true` and the LSN equal to the recorded anchor; otherwise incomplete legacy |
| Any `record_format`, `fingerprint_format`, `item_format`, `chunk_format`, snapshot-position `format` or watermark version above the known one | unknown | refused (`UnsupportedFormat`), record untouched, incident before any row |
| Different stable lineage | foreign | refused (`ConfigChanged`), record untouched (#128 rule) |

**Crash ordering of the replacement:**
- A crash before the CAS leaves the legacy record, and the next start classifies it again.
- A crash after the CAS finds `allocated` `g+1` and resumes it.
- Legacy progress deletion happens after the CAS and is idempotent.
- Old event identities cannot leak: rows of `g+1` carry `g+1`.

**One-time cost:** an interrupted snapshot at upgrade is copied again in full, once (CHANGELOG and upgrade notes).

## 10. Migration compatibility and rollback

**Upgrade.**
- Section 9 applies on the first start; nothing is rewritten before a lineage-verified classification.
- Completed legacy pipelines continue streaming with no recopy.

**Rollback** (to a pre-queue binary):
- **After `completed`:** sink checkpoints are ordinary CDC positions and the old binary resumes normally. The old binary refuses the format 3 control record (`UnsupportedFingerprintFormat`) only on a path that allocates a generation, which a CDC resume does not take.
- **During a snapshot, PostgreSQL:** the old binary reads a snapshot position (no `lsn`) as incomparable and fails closed. It never resumes CDC at the anchor with the snapshot unfinished. The operator either upgrades again or starts a new snapshot explicitly.
- **During a snapshot, MySQL:** the old binary decides on its own progress record, which the upgrade deleted, so it starts a fresh snapshot of a new generation: a full recopy, no loss. Its generation allocation refuses the format 3 record, so it fails closed before any row.
- **Not supported:** resuming a queue generation on an old binary; reading the queue records with an old binary.

## 11. Corruption, missing state, retention and operator recovery

| Condition | Detection | Response |
|---|---|---|
| Undecodable control, item, journal entry or snapshot position | decode error | fail closed before any row, incident (new reason code `snapshot_state_invalid`, action `resnapshot`) |
| Journal gap in `(folded_through, M]` | fold | same |
| Item key of a generation exists at allocation (state lost, ABA) | prefix probe at allocation | same |
| Control missing but sink checkpoints hold snapshot positions | startup | same (never allocate generation 1 again over stale positions) |
| Anchor no longer retained (6.6 precondition fails) | resume preflight | fail closed, incident `snapshot_anchor_unavailable`, action `resnapshot` (Q5) |
| Retention approaching its limit during a snapshot | PostgreSQL: WAL from `A` against `snapshot.max_retention_bytes`; MySQL: the existing purge guard | stop the snapshot with that incident before retention is lost, never silently |
| Schema of a pending item changed (signature mismatch at the resume anchor check) | resume verification | per Q3 |
| Concurrent owner | `run` changed, or a journal conflict | stop, incident `snapshot_state_invalid` (class `concurrent_owner`) |

**Operator recovery** (the recovery CLI PR builds on this):
- `recover diagnose` reads the control record, the item counts by state, the journal window, `M` and the anchor retention.
- `resnapshot` is the generation replacement CAS (`g+1`, `replaced = g`) plus the reset of the source's per-sink checkpoints, under the CLI's proof, actor and reason rules.

There is no automatic recovery from corruption.

## 12. Bounds

- **Workers:** `max_parallel_tables`, fixed.
- **Connections:** workers + 1 coordinator + 1 guard; intra-table parallelism stays within `max_parallel_chunks` per item, and the total is capped (Q6).
- **Resident memory:**
  - one item page (`discovery_page_size`);
  - in-flight chunks (`workers x max_parallel_chunks x chunk_size` rows);
  - the publisher channel;
  - one journal page during a fold.

  It does not grow with the table count. The full plan is never resident.
- **Durable data:**
  - O(tables) items while the generation runs, deleted after completion;
  - an unfolded journal window bounded by the fold cadence plus the unacknowledged window;
  - one control record.
- **Retention of source logs:** bounded by `snapshot.max_retention_bytes` (PostgreSQL) and the binlog purge guard (MySQL). There is no wall-clock maximum (Q6).

## 13. Crash ordering at every boundary

| Step | Durable write | Crash before | Crash after |
|---|---|---|---|
| Allocate | control CAS | no generation; retried | `allocated`, discovery restarts from the beginning |
| Discovery page | items (`slot_create`), then control `discovery_after` | page re-read; existing items accepted when identical, conflict otherwise | next page |
| Seal + anchor | control CAS to `running` | `allocated` unsealed: discovery continues from its last page and the anchor is taken again (the previous PostgreSQL slot is owned and inactive and is recreated, per #127/#123 rules) | `running`; no row yet published |
| Journal chunk | `ChunkEntry(s)` | chunk read again | entry without delivery: `s > M`, read again under a new sequence |
| Publish | (none; channel) | as above | delivered or not; only `M` decides |
| Sink commit | per-sink key (coordinator) | `M` lower: duplicates | `M` higher |
| Fold item | item CAS | entries unfolded; fold repeats | idempotent repeat |
| Fold control | control `folded_through` | items merged, entries kept: repeat is idempotent | entries deletable |
| Delete folded entries | entry deletes | entries kept below `folded_through`: ignored and deleted later | done |
| Rows produced | control CAS with terminal | `running`: last items re-checked from items and journal; terminal recomputed | `rows_produced` |
| Terminal publish / commit | per-sink keys | reconcile: `M` below the terminal, revert to `running` | reconcile: `M` covers it, `completed` |
| Completed | control CAS verifying terminal | reconcile at start | CDC-only from `M` |
| Reclaim | item/entry deletes | repeated | done |
| Legacy replace | control CAS, then legacy progress delete | legacy classified again | `g+1` resumes; delete repeated |

**SQLite `synchronous=NORMAL`.** A power loss drops a suffix of commits. Every write the resume depends on precedes its externally visible effect:
- the journal before publishing;
- the control state before rows;
- a per-sink key after the sink's own commit.

So a lost suffix only lowers `M` or loses unpublished work, which means duplicates, never loss.

## 14. Invariants (each is a test)

- **I1** No event of generation `g` is published unless the control record is `running`, `g` is current, and the anchor is set.
- **I2** Every published chunk has its journal entry, durable, before its first event leaves the publisher.
- **I3** Publish sequences are strictly increasing in send order within a generation, and the delivery task preserves that order.
- **I4** A sink checkpoint `(g, s)` implies that sink received every chunk of `g` with sequence at most `s`.
- **I5** After any crash, every chunk with sequence above `M` (or unjournaled) is read again, and no chunk with sequence at most `M` is read again.
- **I6** `completed` is reached only by a CAS from `rows_produced` with the identical terminal, and only when `M` covers that terminal.
- **I7** A start in `completed` publishes no snapshot row.
- **I8** A start in `rows_produced` with `M` below the terminal reverts to `running` before reading any row.
- **I9** Generations strictly increase; allocation refuses when any key of the new generation exists; no key of a generation is written after the control record has moved past it.
- **I10** Legacy classification uses formats only:
  - incomplete legacy becomes `g+1` after a lineage check, by CAS;
  - demonstrably completed legacy becomes `completed` without recopy;
  - unknown formats and foreign lineage are refused with the record byte-identical.
- **I11** A snapshot-position checkpoint never parses as a stream position in a pre-queue binary (PostgreSQL: no top-level `lsn`).
- **I12** The fold is idempotent, and deleting entries never removes one above `folded_through`.
- **I13** Resident queue and frontier state at 10K tables is within a constant factor of 1K (probe counters).
- **I14** Bytes per boundary checkpoint and per journal entry are independent of the table count.
- **I15** Every condition in section 11 fails before any row is read, with its incident, and leaves the stored records unchanged.
- **I16** A resume reads no row unless the anchor-retention precondition (6.6) held at that start.
- **I17** No snapshot row event identity of generation `g` is emitted with any other generation, and re-reading a chunk reproduces its rows' identities.

## 15. Test plan

**In-memory backend (semantics, concurrency; `FaultBackend`):**
- state machine transitions and refusals (I6, I8, I9, I10, I15);
- a crash at each write of section 13 (the `fail_after_writes_to` crash model), then restart: assert I2, I5, I12;
- concurrent fold and publish;
- a concurrent owner (I15);
- fold idempotence under repetition.

**SQLite (file database):**
- crash/reopen at each step: drop the store, reopen the file, continue;
- lost tail: snapshot the database file mid-run and restore it as an earlier state, emulating `synchronous=NORMAL`, then assert duplicates only;
- corruption: overwrite a control, item and journal value with invalid bytes; delete one journal entry inside the window (gap);
- each corruption is detected and the records are left unchanged.

**PostgreSQL backend (`serial-pg` lane):**
- CAS contention on the control record (one winner);
- journal create-only conflicts;
- `slot_list` key order for the item and journal keys (byte order with `COLLATE "C"`);
- restart from a fresh pool;
- partial failure through `FaultBackend::wrap(pg)` at fold and reclaim steps.

**Engines (Docker suites, one durable backend each):**
- PostgreSQL on SQLite, MySQL on the PostgreSQL backend (Q10):
  - interrupt a multi-table snapshot after some chunks are acknowledged and some are not;
  - restart;
  - assert:
    - zero loss: final sink state equals the source after CDC passes the resume time;
    - only unacknowledged chunks are read again;
    - `rows_produced` then `completed` only after the terminal commit;
    - a second restart is CDC-only.
- Also per engine:
  - anchor retention lost (PostgreSQL: slot advanced past `A`; MySQL: binlog purged) fails closed with its incident;
  - a pending table altered between runs (Q3);
  - an optional sink lagging (8.2);
  - a legacy incomplete generation and a legacy completed one (section 9).
- F-1 regression: a PostgreSQL restart after a committed snapshot chunk, first on the current code to confirm the defect.

**Coordinator:** a data-less boundary batch commits its checkpoint to every succeeded sink under each commit policy, in order after the preceding batches (Q7).

## 16. Acceptance measurements

**Structural CI (small, fast):**
- Snapshots of 10, 30 and 90 tables, each with a fixed number of chunks.
- The probe counts journal writes, item CASes, control CASes, deletes, bytes per boundary, bytes per journal entry and the resident maximum.
- Asserted formula:

  ```text
  durable writes = items (discovery) + chunks (journal) + item folds (<= chunks)
                 + control CASes (<= pages + folds + 4) + deletes (chunks + items)
  ```

- Bytes per boundary are constant across 10/30/90.

**Final candidate only:**
- The 1K/10K measurement on the reference machine (i7-1355U, 12 threads, 31 GB) for both engines.
- Absolute total and phase times, progress plus frontier bytes, peak RSS, boundary counts.
- Gates:
  - bytes 10K/1K at most 12x;
  - wall time at most 15x;
  - resident state flat (I13).

## 17. Proposed implementation commit sequence (one PR)

1. **Queue primitives and formats** (storage adapter `snapshot_queue`):
   - control record format 3, items, journal, fold, reclamation;
   - snapshot checkpoint and watermark formats with their comparators;
   - the format classification of section 9;
   - in-memory, SQLite and PostgreSQL backend tests.
2. **Delivery:** commit the checkpoint of a data-less boundary batch; coordinator tests.
3. **PostgreSQL wiring:**
   - plan into items;
   - journaled publishing;
   - resume with the slot-bounds proof at `A` and a new exported snapshot;
   - JSON snapshot positions (closes F-1);
   - `snapshot.max_retention_bytes`.
4. **MySQL wiring:**
   - the same, with anchor reuse on resume (completes F2) and the purge guard incident;
   - keyless tables chunked by a bounded scan.
5. **Generation completion:** `rows_produced` / `completed` transitions, reconciliation at start, CDC-only start in `completed`, legacy classification and replacement.
6. **Durable-store and scale evidence:** engine interruption scenarios on real backends, structural CI cases, the 1K/10K measurement on the final candidate.
7. **Documentation:**
   - snapshot resume semantics (correcting `mysql.md` and `postgres.md`);
   - the overlap window;
   - upgrade and rollback;
   - new incidents and configuration;
   - capacity envelope;
   - CHANGELOG.

## 18. Questions for the reviewer

- **Q1. F-1 timing.** Confirm the PostgreSQL bare-LSN snapshot checkpoint defect with a regression now and fix it in a small separate PR before the queue (proposed, since it blocks restarts on current releases)? Or fold it into commit 3?
- **Q2. Resume read view.** Accept that a resumed snapshot reads its remaining chunks in a later read view, widening the declared overlap window to `(A, T_last_resume]` (6.6)? The alternative is restarting the generation on every interruption, which loses the purpose of the queue.
- **Q3. Schema change of a pending item between runs.** Proposed:
  - not started (nothing acknowledged): re-plan that item only (new registry version and signature, by item CAS);
  - partially acknowledged: fail closed with `schema_drift_blocked` and recommend `resnapshot`. Continuing a table across a key or identity change could leave rows under a stale key in current-state sinks.

  Alternative: apply `on_schema_drift` (adapt/halt) as CDC does.
- **Q4. Optional sink holding completion.** A permanently failing `required: false` sink keeps the generation in `rows_produced`, re-reads chunks on every restart and retains the anchor. Proposed: an incident `snapshot_completion_blocked` (naming the sink) after a bounded number of restarts or retention threshold. The operator removes the sink or runs `resnapshot`. Alternative: complete on required sinks only and declare that lagging optional sinks miss the remaining snapshot rows.
- **Q5. Anchor retention lost.** Fail closed with `snapshot_anchor_unavailable` and require an operator `resnapshot` (proposed: a full recopy of a large catalog is an operational event)? Or automatically replace the generation with a new anchor?
- **Q6. Configuration.**
  - New `snapshot.max_retention_bytes` (PostgreSQL WAL retained from the anchor). Default proposed: unset, meaning the slot's own `max_slot_wal_keep_size` governs, with the incident at 90% of it when set.
  - A total snapshot connection cap (`max_snapshot_connections`, default workers x `max_parallel_chunks` + 2).
  - No wall-clock maximum duration (retention is the meaningful bound).
- **Q7. Data-less boundary commits.** The delivery task would commit a boundary-only batch's checkpoint after sending an empty batch to every sink. `durable_v2` S3 must record the watermark of an empty batch (a manifest entry with no data object). Acceptable, or should the terminal boundary always ride on a final event (impossible for an all-empty snapshot)?
- **Q8. Ownership.** Accept detection only (`run` token checked at every control CAS, create-only journal) until a lease exists?
- **Q9. Fold cadence.** Every 256 acknowledged entries or 5 seconds, plus at startup.
- **Q10. Engine scenario backends.** PostgreSQL source on SQLite and MySQL source on the PostgreSQL backend, or both engines on both backends for the single interruption scenario?
