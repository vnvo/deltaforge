# Recovery Operations - Design (revision 1)

**Status:** DRAFT for review. Design only.
**Date:** 2026-10-06
**Scope:** rc.1 item `recovery-cli`, after the durable snapshot queue (#132, merged as 2401edd). Operations for rc.1: `resnapshot` and `pg-adopt-timeline`. Deferred: `pg-enable-failover-slot` apply, `kafka-replay-from-checkpoint` (I1d), `new-source-id` (a configuration change: never applied).

## 0. Rulings this design implements (2026-10-04 milestone approval)

1. Recovery runs **in the server, on a stopped, quiescent pipeline**. `deltaforge recover` is a client.
2. **Plan** is read-only and canonical. **Apply** needs the exact proof, an actor and a reason. The server recomputes the plan immediately before applying and refuses any difference.
3. **One durable recovery operation per pipeline**, under CAS. Apply is crash-resumable.
4. Recovery **never silently advances a checkpoint**.
5. Every apply is **audited**.
6. Mutating endpoints are **never anonymous remote admin**: a local admin interface, or a dedicated admin credential, until the API has verified authentication.
7. **No global store gate** for recovery (the server already holds it as `Server`).

## 1. Starting point

- **CLI.** `deltaforge` subcommands (`preflight`, `schema-migrate`, `store-gate`) run offline against the store. There is no HTTP client in the binary.
- **API.** axum on `--api-addr` (default `0.0.0.0:8080`), no authentication. Incident endpoints are read-only plus acknowledgement; `asserted_actor` is recorded unverified, with the peer address as `origin`.
- **Pipeline state** is in memory (`Running`, `Paused`, `Stopped`, `Deleting`; failed is derived from health). Nothing prevents a resume while state is being changed.
- **Incidents.** `Resolution::RecoveryOperation { operation }` exists and is unused. `Transition::Resolved` carries no actor, reason or proof. The recovery epoch is raised only by a verified start.
- **Snapshot queue.** `QueueStore::replace(.., by_recovery)` can replace a blocked or completed generation; production never calls it with `by_recovery` for a blocked one, so a blocked generation stays halted forever.
- **PostgreSQL continuity.** A checkpoint without continuity evidence on a server that has left timeline 1 stops with `pg_continuity_unproven`, class `timeline_unrecorded`. The record that would satisfy the proof is `failover/pg_continuity:{source}` (`ContinuityRecord`, format 1), written today only by the source itself (`kv_put`).
- **Docs** tell operators to `DELETE` checkpoint rows from SQLite to re-snapshot.

## 2. Architecture

### 2.1 Surfaces

| Surface | Where | What |
|---|---|---|
| `GET /recovery/pipelines/{p}` | admin listener | diagnose (read-only) |
| `POST /recovery/pipelines/{p}/plan` | admin listener | plan (read-only), body `{operation, incident?, args}` |
| `POST /recovery/pipelines/{p}/apply` | admin listener | apply, body `{operation, incident?, args, expect_proof, actor, reason}` |
| `deltaforge recover diagnose <p> [--json]` | CLI | client of the above |
| `deltaforge recover plan <operation> --pipeline <p> [--incident <id>] [args] [--json]` | CLI | prints the plan, ending with `proof <sha256>` and the exact apply command |
| `deltaforge recover apply <operation> --pipeline <p> ... --expect-proof <d> --actor <a> --reason <r>` | CLI | applies; prints the outcome and the audit entry |

The CLI takes `--admin-url` (default `http://127.0.0.1:9091`) and `--admin-token-file`.

### 2.2 Admin listener and credential

- A **separate listener**, `--admin-addr`, default **`127.0.0.1:9091`** (loopback). It serves only `/recovery/...`; the public API is unchanged.
- `--admin-token-file`: when set, every admin request needs `Authorization: Bearer <token>` (constant-time compare). The file is read at startup; its content is never logged.
- **Startup refuses a non-loopback `--admin-addr` without a token file.** A loopback address without a token is allowed (local admin interface).
- The audit records the asserted actor, `actor_verified: false` (a token proves possession, not identity), the credential kind (`loopback` or `token`) and the peer address.

### 2.3 Preconditions on the pipeline

A plan may be computed in any state (it reads). **Apply requires the pipeline to be quiescent**:
- status `Stopped`, or failed with its source and coordinator tasks joined;
- no recovery of another operation in progress (section 3).

While an apply runs, the manager holds a per-pipeline recovery lock: `resume`, `start`, config `patch` and `delete` are refused with `409 recovery_in_progress`. A pipeline whose durable recovery record is not finished **does not start** (at server start or on resume): it reports `recovery_pending` in status and readiness, naming the operation and proof.

## 3. Durable recovery record

Slot namespace `recovery`, key `segment(pipeline)` (the incidents escaping).

```json
{
  "format": 1,
  "operation": "resnapshot",
  "pipeline": "orders",
  "source": "orders-pg",
  "proof": "<sha256 hex>",
  "plan": { "...canonical plan, section 4..." },
  "actor": "alice", "actor_verified": false, "credential": "loopback", "origin": "127.0.0.1:53122",
  "reason": "binlog purged during snapshot; recopy approved",
  "started_at_ms": 0,
  "step": 2,
  "state": "applying"
}
```

- **Claim:** `slot_create` when absent, or `slot_cas` from a record in state `completed` (the previous operation's record is kept until replaced: it is the last-operation view of `diagnose`). A record in state `applying` blocks any other claim.
- **Advance:** each step's effect is written first; then `step` is advanced by CAS. Every step is idempotent and verifies its own effect before skipping, so a crash between the two re-runs the step harmlessly.
- **Complete:** after the last step, the incidents named by the plan are resolved (section 6), and the record moves to `completed` by CAS.
- **Resume after a crash:** the pipeline stays stopped (`recovery_pending`). `recover apply` with the **same proof** continues from `step`, using the plan stored in the record (the live state has moved by the operation's own earlier steps, so it is not recomputed); a different proof is refused. There is no abandonment: an unfinished operation is completed, as with schema-migrate.

## 4. Plan and proof

- `CanonicalPlan { domain: "DeltaForge.Recovery.Plan.v1", operation, pipeline, source, bindings, steps, consequences }`, proof = `hex(sha256(serde_json(canonical)))` (the schema-migrate construction).
- **Bindings** (what the proof covers, so that any change between plan and apply refuses):
  - the incident id and its `transition_seq`, when the operation answers an incident;
  - the source lineage (`schema_lineage` record digest);
  - the recovery epoch;
  - every checkpoint key of the source (per-sink and legacy aggregate) with a digest of its bytes;
  - the operation's own state, with versions (below).
- **Steps** are the exact writes, in order. **Consequences** are fixed sentences (for example "every captured table is copied again; sinks receive duplicates").
- Live values that move on their own (WAL flush position, timestamps) are never in the proof; where they matter they are re-checked at apply as preconditions.

## 5. Operations

### 5.1 `resnapshot`

**When:** a blocked snapshot generation (`snapshot_anchor_unavailable`, `snapshot_bound_exceeded`, `snapshot_state_invalid` where the control record is readable); `pg_continuity_unproven` lost classes; `mysql_gtid_position_unavailable` (not `unknown_*`); or an operator's choice on a pipeline with a completed generation. Refused when `snapshot.mode` is `never` (the plan says to change the mode) and when the control record is unreadable or of an unknown format (that stays a manual repair).

**Bindings:** the control record (version, generation, state, chain, blocked reason) and the plan-item count of the current generation.

**Steps:**
1. Control CAS: replace generation `g` by `g+1` with `QueueStore::replace(.., by_recovery = true)`, a new `Allocation::Recovery`, `adoption = pending`, `blocked` cleared; the plan items of `g` are reclaimed after the CAS. Without a control record (a source that never snapshotted under the queue), allocate the first generation.
2. PostgreSQL only, when the slot is lost or invalidated: record that the next start recreates its owned slot (the existing owned-slot re-anchoring; a slot this source cannot prove it owns refuses the plan).

**Checkpoints are not deleted or moved.** The new generation's start barrier moves each sink from its current state into `g+1` (design `snapshot-durable-queue.md` section 5.4: positions of an older generation of the chain and CDC positions of the lineage are accepted). Until a sink has adopted, nothing is resumed from its old position: the snapshot decision precedes the resume-position check, and the stream opens only after the generation, at its anchor.

**Required regression:** for each engine, a source stopped on a lost position (PostgreSQL dropped slot; MySQL purged binlog) and on a blocked generation recovers through `resnapshot` and streams from the new anchor, with every sink checkpoint moved only by the start barrier.

**Consequences:** every captured table is copied again; sinks receive the rows again; the anchor is new.

**Resolves:** the incidents named by the plan, as `RecoveryOperation { operation: "resnapshot", proof }`.

### 5.2 `pg-adopt-timeline`

**When:** `pg_continuity_unproven`, class `timeline_unrecorded` only.

**Plan reads on the source** (a regular connection, then the replication session's `IDENTIFY_SYSTEM`): system identifier, database OID, current timeline, the slot (exists, logical, not invalidated, `restart_lsn`, `confirmed_flush_lsn`, `wal_status`) and the durable checkpoint `F` (every sink's position).

**Preconditions, failing the plan:**
- system identifier and database OID equal the recorded source lineage;
- no continuity record exists for the source;
- the slot exists, is logical and not invalidated, and `restart_lsn <= F` and `confirmed_flush_lsn <= F`;
- every sink checkpoint is unstamped (pre-continuity) and comparable.

**Bindings:** all of the above except the WAL flush position, which apply re-checks (`flush >= F`).

**Step:** create `failover/pg_continuity:{source}` with format 1, a new chain id, the live system identifier, database OID and timeline, `transition_id = 0` and `proven_at = F`, as create-if-absent (a record that appeared since the plan refuses).

**After apply:** the next start runs the full continuity proof against that record (slot bounds, WAL reachability) and stamps the checkpoints into the chain at transition 0 (`adopt_into_chain`); any failure there raises a new incident. Checkpoint positions never change.

**Consequences:** the operator asserts that the history from `F` to now is this timeline's; a same-timeline rewind is not detectable and is unsupported.

**Resolves:** the `timeline_unrecorded` incident, as `RecoveryOperation { operation: "pg-adopt-timeline", proof }`.

**Required regression:** a PostgreSQL 16 source whose server switched timeline before its checkpoint was stamped stops with `timeline_unrecorded`, is adopted, and continues exactly at `F`; adoption refuses on a lineage mismatch, a missing or invalidated slot, and slot bounds past `F`.

## 6. Incidents and audit

- `Resolution::RecoveryOperation` gains `proof` and stays the resolution of every incident a recovery resolves. A new action code `AdoptTimeline` is recommended for `timeline_unrecorded` (in place of `Resnapshot` first).
- **Audit:** a new transition `RecoveryApplied { operation, proof, asserted_actor, actor_verified, credential, origin, reason }` precedes the `Resolved` transition of each incident, through the existing crash-safe `pending_audit` path. The recovery record keeps the same fields for operations that resolve no incident.
- The recovery epoch is not raised by an apply: an apply is an audited operator action, not a verified recovery. The next verified start raises it as today; a start that fails again raises a new incident.
- **Diagnose** lists: pipeline state and quiescence; the recovery record (pending or last completed); open incidents with explanations and evidence; for each, the applicable operations (mapped from its actions and class) or "none in this release".

## 7. Crash ordering

| Step | Write | Crash before | Crash after |
|---|---|---|---|
| Claim | recovery record `applying`, step 0 | nothing changed | pipeline held `recovery_pending`; resume with the same proof |
| Operation step | its own CAS / create | step re-runs | step verified done, skipped |
| Step advance | record CAS | step re-runs (idempotent) | next step |
| Resolve incidents | incident transitions (audited) | re-run (resolving a resolved incident is a no-op) | done |
| Complete | record CAS to `completed` | resolution re-run, then complete | pipeline may start |

## 8. Tests

- Per operation: plan/apply round trip; proof mismatch when state moves between plan and apply (a checkpoint written, the incident reopened, the control record changed); a crash after each step resumes with the same proof and refuses another; the incidents resolve with the audit entry; refusal while the pipeline runs and while another recovery is pending; a pending recovery keeps the pipeline from starting across a server restart.
- Admin listener: refuses a non-loopback address without a token at startup; refuses a missing or wrong token; the public API has no recovery route.
- Recovery record contract on SQLite and PostgreSQL.

## 9. Documentation

- New page "Recovery Operations": diagnose, plan, apply, the admin listener and token, each operation's plan and consequences, crash resume.
- Replace the SQLite `DELETE` advice (`checkpoints.md`, `troubleshooting.md`, PostgreSQL source page) and the "not in this build" notes (`snapshots.md`, `failover.md`, CHANGELOG).

## 10. Commit sequence (one PR)

1. Recovery core: record format, claim/advance/complete/resume, canonical plan and proof, audit transition, `RecoveryOperation { proof }`; contract on both backends.
2. Admin listener, token, pipeline recovery lock and `recovery_pending` start refusal; REST diagnose/plan/apply with an operation registry.
3. CLI client (`deltaforge recover`).
4. `resnapshot` with the cross-engine regressions.
5. `pg-adopt-timeline` with its topology regression.
6. Documentation.

One core gate on the final accepted tree.

## 11. Decisions for the reviewer

1. **Admin access:** loopback admin listener by default, bearer token required for any other address (proposed); or a token always.
2. **`resnapshot` keeps sink checkpoints** and relies on the start barrier to supersede them (proposed); or it also deletes them (simpler to reason about, but it erases the evidence the start barrier checks and loses the per-sink chain ordering).
3. **Crash resume** by an explicit re-apply with the same proof while the pipeline stays held (proposed); or automatic completion at server start.
4. **Scope:** `resnapshot` refuses an unreadable or unknown-format control record (stays manual) (proposed); or it may overwrite one after showing its digest.
