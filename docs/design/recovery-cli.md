# Recovery Operations - Design (revision 2)

**Status:** Directionally approved (2026-10-06); revision 2 applies the reviewer's four decisions and required corrections.
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

### 2.2 Admin listener and credential (decision 1)

- A **separate listener**, `--admin-addr`, default **`127.0.0.1:9091`**, serving only `/recovery/...`. The public API is unchanged and has no recovery route.
- **Loopback only in rc.1.** Startup refuses a non-loopback `--admin-addr` (TLS for the admin listener is not in rc.1; a bearer token over plain HTTP off the host is insufficient). Remote administration uses an SSH tunnel to the loopback port.
- **A token is always required**, loopback included: `--admin-token-file` is mandatory whenever recovery is enabled; without it the admin listener does not start and `deltaforge recover` explains how to configure it.
  - The file must be a regular file owned by the server's user, not readable or writable by group or others (mode `0600` or `0400`); otherwise startup refuses.
  - The token is the file's content with one trailing newline removed; an empty token, or one shorter than 32 bytes, is refused.
  - Requests carry `Authorization: Bearer <token>`; comparison is constant-time; a missing, empty or wrong credential is `401` with no detail.
  - The token is never logged, echoed, stored in the recovery record or included in errors.
- The audit records the asserted actor, `actor_verified: false` (a token proves possession, not identity), `credential: "token"` and the peer address.

### 2.3 Apply sequence and preconditions

A plan may be computed in any state (it only reads). **Apply runs strictly in this order:**

1. **Acquire the per-pipeline recovery lock** in the manager. While held, `resume`, `start`, configuration `patch` and `delete` are refused with `409 recovery_in_progress`.
2. **Confirm quiescence:** status `Stopped`, or failed, with the source and coordinator tasks joined (awaited, not only cancelled).
3. **Read the recovery record** (section 3). If an operation is pending, this is a resume (section 3.2); otherwise:
4. **Recompute the plan and verify the proof** while holding the lock; any difference refuses with the new proof shown.
5. **Claim the recovery slot** (create, or CAS from `completed`).
6. **Only then mutate state**, step by step.

The lock is released after completion or failure. **A pipeline whose recovery record is not `completed` never starts:** at server start and on resume it stays stopped and reports `recovery_pending` (operation, proof, current step) in status and readiness. Server startup never mutates recovery state (decision 3).

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
  "actor": "alice", "actor_verified": false, "credential": "token", "origin": "127.0.0.1:53122",
  "reason": "binlog purged during snapshot; recopy approved",
  "started_at_ms": 0,
  "step": 2,
  "verified": [{ "step": 1, "observed": "post", "digest": "<sha256 of the observed state>" }],
  "outcomes": { "slot": "dropped_owned_slot" },
  "pending_audit": null,
  "state": "applying"
}
```

### 3.1 Claim, steps and completion

- **Claim:** `slot_create` when absent, or `slot_cas` from a record in state `completed` (the last completed record is kept until replaced; diagnose shows it). A record in state `applying` refuses any other claim.
- **Steps:** each step declares its exact **pre-state** and **post-state**. Before acting, the step reads the live state:
  - exactly the pre-state: perform the write (CAS or create-if-absent), then verify the post-state;
  - exactly the post-state: already done, skip;
  - anything else: stop (`recovery_state_diverged`), leaving the record at that step for diagnosis; no further write.

  After the post-state is verified, the record's `step` and `verified` entry advance by CAS.
- **Completion** (the audit ordering): the `RecoveryApplied` transition and every incident resolution named by the plan are written through the incidents' crash-safe `pending_audit` path; the record's `pending_audit` names them before they are written. Only when each is durable (or recorded as pending for the deterministic repair the incidents store already performs) does the record move to `completed` by CAS. A crash in between leaves `applying` with the audit step pending; the resume finishes it.

### 3.2 Resume (decision 3)

- The pipeline stays stopped with `recovery_pending`. The operator re-runs `recover apply` with **the same proof**; any other proof, operation or pipeline is refused.
- The resume uses **the plan stored in the record**, never a recomputation (earlier steps have changed the state the original plan was computed from). Each remaining step applies the pre-/post-state rule above; the step that was in flight is either still in its pre-state (performed now) or in its exact post-state (skipped).
- There is **no abandonment** once mutation began: an operation is completed, or it stops on divergence for manual repair with the record kept as evidence.

## 4. Plan and proof

- `CanonicalPlan { format: 1, domain: "DeltaForge.Recovery.Plan.v1", operation, pipeline, source, bindings, steps, consequences }`.
- **Deterministic serialization:** structs with a fixed field order, every collection sorted (`BTreeMap`, sorted vectors), no floats, no timestamps, integers and strings only; proof = `hex(sha256(canonical JSON bytes))`. A format change bumps `format` and the domain version.
- **Bindings** (any change between plan and apply refuses):
  - the incident id and its `transition_seq`, when the operation answers an incident;
  - the source lineage (`schema_lineage` record digest);
  - the recovery epoch;
  - **the complete sorted set of the source's checkpoint keys** (every per-sink key and the legacy aggregate key, present or absent) with a SHA-256 digest of each value's bytes;
  - the operation's own state, with versions (section 5).
- **Steps** are the exact writes in order, each with its pre-state and post-state digest. **Consequences** are fixed sentences.
- **Diagnostic observations** (the current WAL flush position, slot `active_pid`, timestamps) are printed in a separate `observed` section that is not part of the canonical plan. A value that matters for safety is re-checked at apply as a precondition instead of being bound.

### 4.1 The checkpoint position F

Where an operation needs the source's resume position, F is derived by **the production per-sink comparator** (`PerSinkCheckpointProxy` over the source's own `compare_checkpoints`, as a running pipeline folds it), never a separate implementation. The plan refuses:
- a malformed checkpoint;
- a partial set (a sink of the configured cohort without a checkpoint while others have one);
- a snapshot position, a generation-start (adoption) position or a snapshot-chain position, when the operation needs a stream position;
- a checkpoint of another lineage;
- any incomparable pair.

## 5. Operations

### 5.1 `resnapshot`

**When:** a blocked snapshot generation (`snapshot_anchor_unavailable`, `snapshot_bound_exceeded`); `pg_continuity_unproven` lost classes; `mysql_gtid_position_unavailable` (not `unknown_*`); or an operator's choice on a pipeline with a completed generation.

**Refused (manual repair, decision 4):** `snapshot.mode: never` (the plan says to change the mode); a control record that is unreadable, corrupt or of an unknown format, whatever its digest (`snapshot_state_invalid` stays manual); snapshot-chain or generation-start positions in the checkpoints while the control record is missing.

**Bindings:** the control record (version, generation, state, chain, blocked reason) or its verified absence; the plan-item count of the current generation; on PostgreSQL, the slot ownership record and the slot's state.

**Steps:**
1. **Generation.** Pre-state: the bound control record. Post-state: generation `g+1` of the same chain, `allocated`, `adoption = pending`, `blocked` cleared, `replaced = g`, allocation `Recovery` (`QueueStore::replace(.., by_recovery = true)`). Without a control record, a first generation is allocated, but **only when the checkpoints are empty or are all ordinary, lineage-verified CDC positions**. The plan items of `g` are reclaimed after the CAS (idempotent).
2. **PostgreSQL slot (only when the slot is lost or invalidated).** An explicit, recorded outcome: the slot is dropped only when the slot ownership record proves this source created it (`slot_owner:{source}`, the existing owned-slot proof) and it is inactive; the record's `outcomes.slot` names what was done (`dropped_owned_slot`, `absent`). A slot whose ownership cannot be proven refuses the plan; recovery never recreates or replaces it. The next start creates the new slot through the existing creation path.

**Checkpoints are neither deleted nor advanced (decision 2).** They stay as durable evidence. The new generation's start barrier moves each sink from its current state into `g+1` (`snapshot-durable-queue.md` section 5.4); until a sink adopts, nothing resumes from its old position (the snapshot decision precedes the resume-position check, and the stream opens after the generation, at its anchor).

**Required regression:** for each engine, a source stopped on a lost position (PostgreSQL dropped slot; MySQL purged binlog) and on a blocked generation recovers through `resnapshot` and streams from the new anchor, every sink checkpoint changed only by the start barrier.

**Consequences:** every captured table is copied again; sinks receive the rows again; the anchor is new.

**Resolves:** the incidents named by the plan, as `RecoveryOperation { operation: "resnapshot", proof }`.

### 5.2 `pg-adopt-timeline`

**When:** `pg_continuity_unproven`, class `timeline_unrecorded` only.

**Identity checks run on one gated replication session** (the vendored session the source proves continuity on), at plan time and again at apply time:
- `IDENTIFY_SYSTEM`: system identifier, current timeline, database (OID resolved on the same session);
- server version and `pg_is_in_recovery()`: **a standby is refused**;
- the slot row: exists, logical, not invalidated, `wal_status`, `restart_lsn`, `confirmed_flush_lsn`, `active`, `active_pid`: **a slot active under another consumer is refused**;
- the current WAL flush position.

**Preconditions:**
- system identifier and database OID equal the recorded source lineage;
- no continuity record exists for the source;
- F (section 4.1) is a stream position; every sink checkpoint is unstamped (pre-continuity), well-formed, of this lineage and comparable;
- at apply: `restart_lsn <= F`, `confirmed_flush_lsn <= F` and WAL flush `>= F`.

**Bindings:** system identifier, database OID, timeline, server major version, slot name and its `restart_lsn` and `confirmed_flush_lsn`, F, the checkpoint key set and digests. The WAL flush position is observed and re-checked at apply, not bound.

**Step:** pre-state: no record at `failover/pg_continuity:{source}`; post-state: the record with format 1, a new chain id, the live system identifier, database OID and timeline, `transition_id = 0`, `proven_at = F` (written create-if-absent; a record that differs from the expected post-state stops the operation).

**After apply:** the next start runs the full continuity proof against that record and stamps the checkpoints into the chain at transition 0 (`adopt_into_chain`); any failure raises a new incident. Checkpoint positions never change.

**Consequences:** the operator asserts that the history from F to now is this timeline's; a same-timeline rewind is not detectable and is unsupported.

**Resolves:** the `timeline_unrecorded` incident, as `RecoveryOperation { operation: "pg-adopt-timeline", proof }`.

**Required regression:** a PostgreSQL 16 source whose server switched timeline before its checkpoint was stamped stops with `timeline_unrecorded`, is adopted, and continues exactly at F; adoption refuses a lineage mismatch, a standby, an active slot, a missing or invalidated slot, slot bounds past F, WAL flush behind F, and malformed, partial, snapshot or foreign checkpoints.

## 6. Incidents and audit

- `Resolution::RecoveryOperation` gains `proof` and stays the resolution of every incident a recovery resolves. A new action code `AdoptTimeline` is recommended for `timeline_unrecorded` (in place of `Resnapshot` first).
- **Audit:** a new transition `RecoveryApplied { operation, proof, asserted_actor, actor_verified, credential, origin, reason }` precedes the `Resolved` transition of each incident, through the existing crash-safe `pending_audit` path. The recovery record keeps the same fields for operations that resolve no incident.
- The recovery record is completed only after these audit transitions are durable or pending (section 3.1).
- The recovery epoch is not raised by an apply: an apply is an audited operator action, not a verified recovery. The next verified start raises it as today; a start that fails again raises a new incident.
- **Diagnose** lists: pipeline state and quiescence; the recovery record (pending or last completed) with the **current step, the last verified pre- or post-state and its digest**, and the **safe remediation** (re-apply with the same proof; on divergence, the state that was expected and what was found, with no abandonment); open incidents with explanations and evidence; for each, the applicable operations or "none in this release".

## 7. Crash ordering

| Step | Write | Crash before | Crash after |
|---|---|---|---|
| Lock, quiescence, proof | none (in memory) | nothing changed | nothing changed (the lock is in memory) |
| Claim | recovery record `applying`, step 0 | nothing changed | pipeline held `recovery_pending`; re-apply with the same proof |
| Operation step | its own CAS / create | resume finds the pre-state: performed | resume finds the post-state: skipped |
| Step advance | record CAS (`step`, `verified`) | resume re-verifies the step | next step |
| Audit | `pending_audit` in the record, then the incident transitions | resume writes them | repair or resume completes them |
| Complete | record CAS to `completed` | resume completes | pipeline may start |

## 8. Tests

- Apply ordering: a resume or start attempt during apply is refused; apply on a running or not-yet-joined pipeline is refused before any read of the recovery slot.
- Resume: a crash after each step resumes from the stored plan; a step found in neither its pre- nor post-state stops without writing.
- Per operation: plan/apply round trip; proof mismatch when state moves between plan and apply (a checkpoint written, the incident reopened, the control record changed); a crash after each step resumes with the same proof and refuses another; the incidents resolve with the audit entry; refusal while the pipeline runs and while another recovery is pending; a pending recovery keeps the pipeline from starting across a server restart.
- Admin listener: refuses a non-loopback address; refuses to start without a token file, with a group- or world-accessible file, or with an empty or short token; refuses a missing, empty or wrong bearer token; never logs the token; the public API has no recovery route.
- Proof determinism: the same state yields the same proof across processes and backends; observations outside the canonical plan never change it.
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

## 11. Decisions (ruled 2026-10-06)

1. Admin authentication: a token always, loopback included; loopback-only listener in rc.1 (no TLS); SSH tunnel for remote administration.
2. `resnapshot` keeps checkpoints; the generation start barrier supersedes them; nothing is deleted or advanced.
3. Crash recovery by an explicit re-apply with the same proof; startup only exposes `recovery_pending` and refuses pipeline start.
4. Unknown or corrupt control records fail closed and stay a manual repair.
