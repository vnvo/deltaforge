# Initial Snapshots

An initial snapshot copies the existing rows of every captured table, then hands over to the change stream at a single anchor. This page covers what PostgreSQL and MySQL share: generations, restarts, completion under the commit policy, bounds and blocking, connection limits, the upgrade from earlier releases, and measured scale. The engine-specific mechanics (anchors, locks, privileges) are on the [MySQL](sources/mysql.md#snapshot-initial-load) and [PostgreSQL](sources/postgres.md#snapshot-initial-load) pages.

## Generations

Each snapshot is a **generation**: one complete copy of one sealed table plan, read in one database read view and anchored at one stream position.

- The source discovers its tables in catalog pages and writes each table to a durable plan in the state store. The plan is sealed at the anchor and never changes.
- Before any row of a generation is published, every sink of the pipeline moves its own checkpoint into that generation (the **start barrier**). Every sink must accept, including optional ones. A sink that is unavailable, or whose stored position cannot move into the generation (another snapshot chain, a later generation, an unknown format), fails the start: the pipeline stops, and each later start tries again with a new generation.
- Rows are published chunk by chunk. Each boundary records a constant-size snapshot position, not a per-table cursor.
- When every table is read and every final check passed, the source sends the **terminal barrier**, and each sink commits it once all earlier rows are durably delivered.
- The generation is **completed** when the sinks that acknowledged the terminal satisfy the commit policy. The stream then continues from the anchor.

Generation numbers never repeat. Generations of one source belong to a **snapshot chain**, so a sink can tell a newer generation from an older one; positions of different chains are never compared.

## Restarts copy the whole snapshot again

A generation lives and dies with its read view. **An interrupted snapshot is never resumed table by table or chunk by chunk**: a process restart, a lost worker connection or a lost read view replaces the generation, and the next generation reads every table again under a new anchor. Sinks receive the rows already sent once more (at-least-once; never loss). The replacement raises a non-blocking `snapshot_replaced` incident.

The exceptions:

- If the terminal barrier was already acknowledged by enough sinks to satisfy the frozen policy (a crash after the acknowledgement), the restart **completes the generation without copying** and streams.
- A **blocked** generation is never replaced automatically (see [Blocking](#bounds-and-blocking)).
- A **completed** generation streams changes and never copies rows, except with `snapshot.mode: always` (a new generation on every start).
- A change to the sink cohort or commit policy before completion replaces the generation (`snapshot_replaced`, class `policy_changed`): a terminal is only judged by the cohort that started it.

Plan restart time accordingly: a crash late in a long snapshot costs a full copy.

## Completion and lagging sinks

At allocation the generation freezes the pipeline's commit policy: the mode, the quorum and every sink with its `required` flag. Completion is judged only against that frozen cohort:

| Policy | The generation completes when |
|---|---|
| `all` | every sink has committed the terminal |
| `required` (default) | every `required: true` sink has |
| `quorum` | at least `quorum` sinks have |

The completion record keeps the acknowledging sinks; later configuration changes never reinterpret it.

A sink that **lags**, failing or missing a batch of the generation, does not hold completion under `required` or `quorum`:

- A sink that missed any batch since the generation started **cannot acknowledge its terminal**: it would claim rows it never delivered. It is left behind the terminal.
- Once the generation completes, every sink behind it gets a non-blocking `sink_snapshot_incomplete` incident naming the sink and generation, with the action `rebootstrap_sink`. It is **excluded from the resume position**, so it never pulls the stream back into the snapshot, and it continues with changes from the resume position. **It does not receive the rest of that snapshot**: give it a fresh baseline (a new generation, or an out-of-band load).
- Under `all`, a failing sink stops the pipeline instead. Once the sink recovers and the pipeline resumes, the next start replaces the generation and every sink receives the whole snapshot.

A sink added after completion starts behind it and is treated the same way.

## Bounds and blocking

Every generation runs inside explicit bounds. Crossing 80% of a bound raises a non-blocking `snapshot_bound_warning`. Crossing the bound stops the generation **before** anything is lost:

| Bound | Setting (default) | At the bound |
|---|---|---|
| Anchor age (the read view held open, the log retained after the anchor) | `snapshot.max_anchor_age_secs` (86,400 = 24 h) | blocked, `snapshot_anchor_unavailable` (class `anchor_age`) |
| Plan storage (durable plan in the state store) | `snapshot.max_plan_bytes` (256 MiB), `snapshot.max_plan_items` (1,000,000), checked during discovery | blocked before the plan is sealed, `snapshot_bound_exceeded` |
| PostgreSQL WAL retention of the snapshot's slot | from `pg_replication_slots` (`wal_status`, `safe_wal_size`) | slot lost, invalidated or missing: blocked, `snapshot_anchor_unavailable` |
| MySQL binlog retention | the anchor's binlog file and GTID set still on the server | anchor file purged: blocked, `snapshot_anchor_unavailable` |

The plan bound is the state store's resource; WAL and binlog retention are the source's. They are separate settings and separate incidents.

Two related stops are not bounds:

- Corrupt or unknown-format snapshot state stops the source with `snapshot_state_invalid` and is left untouched; every start stops on it again.
- A second process found owning the source before a publish (`concurrent_owner`) stops only the stale run, with a non-blocking incident; it never changes the current owner's generation. One owner per source remains a deployment requirement (see [Supported Deployment Envelope](deployment-support.md)).

**A blocked generation stays halted across restarts.** Every start re-raises its incident and stops before any row or replacement. These failures need an operator decision (raise the bound, narrow the tables, restore retention), so nothing retries them automatically.

**Recovery** is an explicit, proof-bound `resnapshot` that replaces the blocked generation with a new one, audited with actor and reason. It is part of the recovery CLI, which is required for rc.1 but **not in this build**. Until it ships, a blocked generation stays halted.

## Connection limits

Every connection a snapshot opens (PostgreSQL coordinator and workers; MySQL lock connection, binlog guard and workers; intra-table readers) is taken from one process-wide pool:

- `--max-snapshot-connections` (default 64) caps snapshot connections across every pipeline of the process.
- `snapshot.max_snapshot_connections` caps one source's share (default `max_parallel_tables x max_parallel_chunks + 2`).
- A snapshot takes its base connections (3 on either engine) atomically, before opening any transaction, lock or anchor. Extra workers are taken when free and released when done; the base worker always progresses.
- A snapshot that cannot get its base **queues, holding nothing**, and can be stopped while queued. Queued snapshots are visible in `deltaforge_snapshot_queued{pipeline}`; the pool in `deltaforge_snapshot_connection_permits{state}`.
- A source whose share cannot hold its base, or whose share exceeds the process cap, fails its configuration check.

Oversubscribing the process cap with many sources is allowed: their snapshots queue.

## Upgrading from an earlier release: one-time recopy

Snapshot state from releases before durable generations is classified at the first start after the upgrade, with its source lineage verified first:

- **Proven complete** (the sink checkpoints prove the completion under the commit policy): upgraded in place; nothing is copied.
- **Not proven** (an interrupted snapshot, sinks behind its anchor, a MySQL progress record without its anchor, and every PostgreSQL snapshot taken before the slot-anchor hardening): **copied once more, in full**, as the first generation, before streaming continues. A non-blocking `snapshot_replaced` incident (class `legacy_recopy`) records it.

**This recopy reads every table again and can take as long as the original initial load.** Plan the upgrade window for large catalogs. With `snapshot.mode: never` an unproven snapshot stops the source with an actionable error instead. State of an unknown format is refused and left untouched.

**Rollback** to an earlier release is safe only once every sink checkpoint of the source is a stream position (after completion, and after every sink committed a change past it). Before that, an earlier release refuses the new state rather than misreading it.

## Scale

### What is bounded

The durable queue bounds the **snapshot's own state**, not the whole process:

- **Per boundary:** one constant-size position (about 190 bytes on PostgreSQL, 240 on MySQL), independent of the table count. No per-table cursor vector and no progress record are written.
- **Durable operations per generation** (counted on the storage path at 10, 30 and 90 tables): the control record is created once and updated 4 times; each table's plan entry is written once and deleted once after completion; one control read per publish (every chunk and both barriers) checks ownership; a constant number of other control reads; plan reads are whole page walks, linear in pages.
- **Resident queue state:** at most one discovery page of plan entries (`discovery_page_size`, default 1,000) and at most `max_parallel_tables` table tasks (default 8). The plan itself stays in the state store.

The **whole process** is not bounded by the queue. Schema caches and schema registry state grow with the tables a source uses, each within its own bounds (see [Capacity envelope](capacity-envelope.md#schema-registry-at-scale)). Do not read the figures below as constant total memory.

### 1K and 10K tables

Measured once on the release candidate: single-row tables, default snapshot settings, a debug build, SQLite state store and schema registry, PostgreSQL 17 and MySQL 8.4 in local containers, on an Intel i7-1355U laptop (12 threads, 31 GB). Indicative, not a guarantee:

| Engine | Tables | Total | Preparation | Row copy | Boundaries (bytes) | Plan (stored) | Peak RSS (before) |
|---|---|---|---|---|---|---|---|
| PostgreSQL | 1,000 | 55.4 s | 14.5 s | 40.3 s | 1,000 (0.2 MiB) | 216 KB | 100.9 MiB (98.2) |
| PostgreSQL | 10,000 | 582.0 s | 168.7 s | 410.8 s | 10,000 (1.8 MiB) | 2.17 MB | 103.6 MiB (101.0) |
| MySQL | 1,000 | 12.3 s | 11.0 s | 0.4 s | 1,000 (0.2 MiB) | 202 KB | 51.3 MiB (33.4) |
| MySQL | 10,000 | 149.8 s | 133.6 s | 9.5 s | 10,000 (2.3 MiB) | 2.04 MB | 99.8 MiB (51.4) |

No progress writes in any run. Acceptance gates (10K against 1K), all met:

| Gate | PostgreSQL | MySQL |
|---|---|---|
| Boundary and progress bytes at most 12x | 10.00x | 10.08x |
| Wall time at most 15x | 10.51x | 12.22x |
| Resident queue state flat (discovery page, table tasks) | 1,000 / 8 at both sizes | 1,000 / 8 at both sizes |

Peak RSS growth during the snapshot, reported, not gated: PostgreSQL 2.8 MiB at 1K and 2.5 MiB at 10K; **MySQL 17.9 MiB at 1K and 48.4 MiB at 10K (2.7x)**. The MySQL growth is schema state, not queue state: each table's schema is loaded once while the plan is prepared, into the source's schema cache (up to 4,096 tables or about 64 MiB) and the schema registry's cache, plus a compact per-table record. It grows with the tables captured, within those caches' bounds.

For comparison, the same measurement before the durable queue (in-memory checkpoint store, same machine) took 505 s for 10,000 MySQL tables and wrote 9.6 GiB of boundaries and 1.2 GiB of progress.

### PostgreSQL catalog queries

The measurement also found a quadratic cost in PostgreSQL schema loading: the query reading a table's columns through `information_schema.columns` scanned the whole catalog on every call (44 ms per table at 10,000 tables). It now also filters by the requested schema and table names, so PostgreSQL resolves the table through its catalog name index: 0.4 ms per table, with identical results. Preparing 10,000 PostgreSQL tables went from 440 s to 169 s. The same query serves CDC schema loads.

### What is not claimed

These measurements cover 1,000 and 10,000 tables per source. **Qualification at up to 400,000 tables per source, and for fleets of 50 to 120 million tables, is a separate milestone after rc.1.** It is not a current support claim. See [Capacity envelope](capacity-envelope.md) for the supported starting range.
