# Start-up Capture Boundary - Design

**Status:** Implemented on branch `startup-capture-boundary`.
**Date:** 2026-11-01
**Scope:** the start contract of MySQL and PostgreSQL pipelines: when a pipeline reports `running`, what that guarantees, and the boundary it exposes. Found by the stage 2 scaling study (a marker row written after `running` was not captured in 14 of 32 runs on the PostgreSQL state store).

## 0. Rulings this design implements (2026-11-01)

1. `starting` → `running`: `running` is published only after a capture boundary is fixed and protected against start-up retries. MySQL persists it durably; PostgreSQL establishes the slot and the matching durable resume state. A failed stream-open attempt never chooses a later boundary.
2. Snapshot modes: the anchor is the boundary only once the snapshot/CDC handoff guarantees coverage from it, including recovery after failure. Acquiring an anchor is not enough.
3. Restarts use the same lifecycle and expose the existing resume boundary; it is never replaced by the current tail.
4. Status, the status gauge and `/ready` agree; `/ready`'s aggregation is documented. Liveness (`/health`) is unchanged.
5. `capture_start` is typed, source-specific, with its lineage and meaning. It does not imply immediate delivery or capture of tables excluded by configuration.
6. The MySQL boundary is read with one supported status statement whose consistency is verified, GTID on and off. No fallback to two independently sampled positions.
7. Readiness belongs to the current run generation: a late signal from a cancelled or replaced source never marks its replacement running.
8. Becoming ready does not wait for a data event: an idle source becomes ready.

## 1. Starting point (775c6ec3)

- `PipelineManager::spawn_pipeline_registered` builds the runtime with `status: Running` right after `Source::run`, which only spawns the source task, and sets `deltaforge_pipeline_status` to 1. The API returns `running` at once.
- The source fixes its start point later, in its task: MySQL reads the binlog tail (`SHOW BINARY LOG STATUS`, then a separate `@@gtid_executed` read) after its start-up store reads, inside a retry loop; PostgreSQL creates its slot (`ensure_slot_and_publication`), also inside a retry loop.
- `SourceReady` (marked after the stream opens) feeds only incident recovery. Neither the status nor `/ready` reflects it.
- The start point is persisted (`initialize_sink_checkpoints`) before the stream opens, so restarts resume exactly: the gap is the window between `running` and that point on a pipeline's first start, plus any start-up retries.

## 2. The capture boundary

**Definition.** A run's capture boundary is a source position `B` such that every change to a configured table committed after `B` is delivered under the pipeline's delivery guarantees, and no change committed at or before `B` is delivered as a change event (a snapshot delivers rows as of its anchor instead).

**Fixed and protected** means: `B` is durable, and every later attempt of this run and every restart starts at `B` (or, after progress, at a later committed checkpoint whose changes were delivered), never at a newer tail.

| Start | Boundary `B` | Fixed and protected when | `capture_start.origin` |
|---|---|---|---|
| Restart from a committed position | the committed resume position | already durable; published once identity is verified, the start checkpoints are initialized and the stream is open | `resume` |
| First start, no checkpoint, no snapshot (`never`, or `initial` on a source that already streams without one) | MySQL: one `performance_schema.log_status` read; PostgreSQL: the new slot's consistent point | MySQL: the start checkpoint is persisted for every sink; PostgreSQL: the slot exists and the start checkpoint is persisted for every sink. Published once the stream is open | `first_start` |
| Stream after a completed generation (no sink holds a stream position) | the generation's anchor | the generation is durably `completed` | `snapshot` |
| A snapshot generation runs | the generation's anchor | the generation is durably `completed`: the frozen commit policy's frontier covers its terminal and `completed` is recorded. Before that a failed run replaces the generation (a new anchor), so the anchor alone is not protected | `snapshot` |
| PostgreSQL resume moved by a recorded publication maintenance decision (`abandon_to`) | the decision's position | already durable (the decision is recorded) | `maintenance` |

A source becomes ready at the published point above whether or not any data arrives.

## 3. Status, gauge and `/ready`

- **Status** (`PipeInfo.status`): `starting` while the operator intent is run and the current runtime's boundary is not yet published; then `running`. `paused`, `stopped`, `deleting` and `failed` are unchanged; `failed` wins over every other state. A pipeline paused while starting reports `paused` and returns to `starting` or `running` on resume.
- **Gauge** `deltaforge_pipeline_status`: `1` running, `0.25` starting, `0.5` paused, `0` stopped, `-1` failed. It moves to `1` only when the current runtime publishes its boundary.
- **`/ready`**: `503` while any pipeline is failed, has a blocking incident, has a pending recovery, or is `starting`; `200` otherwise. Paused and stopped pipelines do not hold readiness back (they are operator intent). The body lists the pipelines holding it back with their status. A snapshot-mode pipeline is `starting` until its generation completes, so `/ready` stays `503` for that whole time.
- **Liveness** `/health`: unchanged (process alive). In Kubernetes a `503` from `/ready` removes the pod from the Service's endpoints (for the whole of an initial snapshot) and never restarts it; the chart has no startup probe, and its liveness probe stays on `/health` (`helm_probe_config`).

## 4. `capture_start`

`PipeInfo.capture_start`, present once the boundary is published, typed per source:

```json
{"source": "mysql", "origin": "first_start", "lineage": "<registry lineage hash>",
 "server_uuid": "…", "file": "mysql-bin.000007", "pos": 685, "gtid_set": "…:1-721031"}
{"source": "postgres", "origin": "resume", "lineage": "<registry lineage hash>",
 "system_identifier": 7412…, "slot": "df_slot", "lsn": "0/16B3780"}
```

Meaning: changes to the pipeline's configured tables committed after this position are captured. It promises nothing about delivery latency, tables excluded by configuration, or delivery while paused. In GTID mode the MySQL boundary is the GTID set; `file`/`pos` are the position consistent with it.

## 5. The MySQL boundary statement

`SELECT SERVER_UUID, LOCAL FROM performance_schema.log_status` (MySQL 8.0.14+, 8.4 validated). MySQL 8.4 documents that its `gtid_executed` "is consistent with the binary_log_file and binary_log_position keys" and that the server blocks logging while populating it. `SHOW BINARY LOG STATUS` documents no such guarantee.

Verified on 8.4.9 under 4 concurrent writers (evidence `mysql-log-status-verification-20261101`): `log_status` 0/400 inconsistent pairs; `SHOW BINARY LOG STATUS` 394/400 (position up to 4 transactions past its own GTID set); GTID off: `gtid_executed` is `""` and 400/400 positions fall on transaction boundaries.

Privileges, needed only at a first start without a checkpoint: `BACKUP_ADMIN` and `SELECT ON performance_schema.log_status`. Without them the source stops with a typed error naming both (MySQL errors 1227 and 1142); there is no fallback. The server UUID must match the verified server.

Outstanding qualification: the consistency and privilege behaviour are verified on MySQL 8.4.9 only. Other supported versions and managed services (whether `BACKUP_ADMIN` and `performance_schema.log_status` are available to the CDC user) remain to be qualified.

## 6. Lifecycle

- One boundary signal per runtime, created with the source handle; it is also the run's identity. The manager's watcher applies it only if the runtime it was created for is still the pipeline's runtime (pointer identity): a late signal from a stopped, failed or replaced runtime is ignored.
- The status gauge is never written from a value computed outside the manager's exclusive lock. Every writer acting for a run (the boundary watcher, the source and coordinator monitors, registration) computes the value from the runtime's current state and sets it inside that lock, gated by the run's identity (`publish_run_status`); a failure transition (`PipelineHealth::exit`) completes before its writer takes the lock. The last writer therefore computes after every earlier transition: a `running` computed before a failure can never land after the failure's `-1`, and an old run never writes its replacement's gauge. Operator calls (pause, resume, stop) recompute from the current runtime under the same lock.
- Cancellation during start-up leaves the boundary unpublished; the pipeline reports `stopped`.
- A source that fails after persisting the boundary but before the stream opens reports `failed`; the next start resumes from the persisted boundary (`origin: resume`).

## 7. Tests

- Core: the boundary signal (publish once, wait, idle); the status derivation; the stale-signal rule; `/ready` aggregation; a source or coordinator failure inside the boundary watcher's critical section (gauge -1, API `failed`, `/ready` 503) and an old run's writes after its replacement (none reach it).
- End to end (MySQL and PostgreSQL, through `PipelineManager`): a one-shot hold before the boundary shows `starting`; a row written immediately after `running` is delivered; start-up retries keep `starting`; a stop while starting gives `stopped` and no late `running`; a failure between persisting the boundary and opening the stream resumes at the persisted boundary, with rows written meanwhile delivered; an idle source becomes `running`; a snapshot-mode pipeline is `starting` until its generation completes; MySQL with GTID off; a MySQL user without the `log_status` privileges fails with the typed error.
