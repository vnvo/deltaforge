# Recovery Operations

Some failures stop a pipeline in a way no restart can fix: a snapshot generation that blocked on a bound, a lost replication position, a PostgreSQL source whose checkpoints predate continuity records on a server that has switched timeline. For these, DeltaForge has **recovery operations**: explicit, reviewed, audited changes to the pipeline's durable state, applied by the running server to a stopped pipeline.

Every operation is planned first. The plan shows exactly what will change, what is kept, and the consequences, and ends with a **proof**: a digest of everything the plan was computed from. Apply takes that proof, recomputes the plan, and refuses to write anything if the state moved in between. The state is never edited by hand: there is no supported way to delete checkpoint or snapshot records from the state store, and doing so can lose data.

Operations in this release:

| Operation | For | What it changes |
|---|---|---|
| `resnapshot` | a blocked snapshot generation, a lost replication position, or an operator's decision to copy everything again | allocates the next snapshot generation, so the next start copies every table again |
| `pg-adopt-timeline` | PostgreSQL `pg_continuity_unproven` with class `timeline_unrecorded` | records the server's current timeline for checkpoints written before continuity was recorded |

## Setting up access

Recovery is served on a **separate admin listener**, never on the public API.

- `--admin-addr` (default `127.0.0.1:9091`). **Loopback only**: the server refuses to start with any other address. For remote administration, open an SSH tunnel to the host's loopback port (for example `ssh -L 9091:127.0.0.1:9091 host`) and point the CLI at the local end.
- `--admin-token-file`: **required**; without it the admin listener does not start. The file must:
  - be a regular file, not a symlink;
  - not be readable or writable by group or others (`chmod 600` or `400`);
  - contain one token of at least 32 bytes, with no carriage return, NUL or inner newline (one trailing newline is allowed and is not part of the token).

  The server reads it at startup. The token is never logged or returned in errors. Requests must also come from a loopback peer; forwarded headers are never trusted.

```bash
head -c 32 /dev/urandom | base64 | tr -d '\n' > /etc/deltaforge/admin.token
chmod 600 /etc/deltaforge/admin.token
deltaforge --config pipelines/ --admin-token-file /etc/deltaforge/admin.token
```

The CLI reads the token from a file too (never from the command line) and talks to loopback `http` only:

```bash
deltaforge recover --admin-token-file /etc/deltaforge/admin.token diagnose orders
```

`--admin-url` defaults to `http://127.0.0.1:9091`.

## The workflow

1. **Stop the pipeline** (`POST /pipelines/{name}/stop`), or let it fail. Apply needs it quiescent: stopped or failed, with its tasks finished.
2. **Diagnose** (read-only): the pipeline's state, any recovery operation in progress, open incidents with their explanation and the operations that answer them.

   ```bash
   deltaforge recover --admin-token-file T diagnose orders
   ```

3. **Plan** (read-only). Name the incident the operation answers:

   ```bash
   deltaforge recover --admin-token-file T plan resnapshot --pipeline orders --incident <id>
   ```

   The plan prints its bindings, steps (each with the exact state it expects before and after), consequences, values observed but not part of the proof, whether apply is possible right now, the apply command, and, last, `proof <sha256>`.

4. **Review** the plan, especially its consequences.
5. **Apply** exactly that plan, with your name and reason:

   ```bash
   deltaforge recover --admin-token-file T apply resnapshot --pipeline orders \
     --incident <id> --expect-proof <sha256> --actor alice --reason "binlog purged during snapshot"
   ```

   Apply recomputes the plan while holding the pipeline's recovery lock. If the proof differs, nothing is written and the new proof is shown: review it with `plan` again. It is never used automatically.

6. **Resume the pipeline explicitly** (`POST /pipelines/{name}/resume`). Apply never starts the pipeline.

While an operation is being applied, or is unfinished, the pipeline cannot be started, resumed, paused, patched or deleted. Its status shows `recovery_pending` and `/ready` reports it not ready.

## Outcomes, timeouts and exit codes

| Exit code | Meaning |
|---|---|
| 0 | done |
| 2 | invalid input, configuration or authentication; unknown pipeline, operation or incident; operation not applicable (for example `snapshot.mode: never`) |
| 3 | proof mismatch: the state changed since the plan. Review the new plan |
| 4 | pipeline state: not stopped yet, being deleted, another request in progress, or a source precondition (for example a standby endpoint, or an active slot) |
| 5 | a recovery operation is pending, diverged, stopped, or the state needs manual repair |
| 6 | transport or server failure, malformed response, or timeout: **the outcome may be unknown** |

The CLI waits up to 310 seconds for an apply. The operation itself runs to its end on the server even if the request is abandoned. **Apply is never retried automatically.** After a timeout or a lost connection, run `diagnose`: if the operation completed, re-applying the same proof reports it as already completed and writes nothing; if it is still pending, re-apply the same proof.

## Pending and diverged operations

An operation is recorded durably before its first write, step by step. Every step names the exact state it changes and the state it leaves. If the server stops mid-operation:

- the pipeline stays stopped (`recovery_pending`) across restarts; the server never continues or abandons it on its own;
- **re-apply with the same proof** to finish it. The resume uses the plan stored with the operation (never a recomputed one). Each remaining step finds either its expected starting state (and performs it) or its expected result (and skips it).

If a step finds neither, the operation **diverges**: it stops without writing anything further and keeps the record as evidence. `diagnose` shows the step, the expected states and what was found, and the next safe action. Bring that state back to the expected starting state, then re-apply the same proof. There is no abandonment once writing began.

## `resnapshot`

Allocates the next snapshot generation so the next start copies every table again under a new anchor (see [Initial Snapshots](snapshots.md)).

**Consequences** (printed first in every plan):
- **The complete snapshot is copied again.** Every captured table is read in full.
- **Duplicates are expected.** Sinks receive every row again (at-least-once).
- **Sink checkpoints are kept**, never deleted or advanced. Each sink moves into the new generation through the generation's start barrier.
- The pipeline is not started; only the incident named in the plan is resolved.

**What it writes:**
- the next generation of the same snapshot chain (or, for a source without snapshot state, a first one), marked as a recovery allocation, unblocked, its start barrier pending, freezing the configuration of now (table patterns, commit policy and cohort). The next start runs it in place; if the configuration or source changed before then, that start replaces it as usual;
- the replaced generation's plan is reclaimed;
- **PostgreSQL slot.** A slot this source created and lost is never recreated implicitly:
  - lost or invalidated and provably this source's (finalized ownership record for this source, pipeline, server, database, slot and plugin): the plan drops it as an explicit step;
  - absent, or dropped by the plan: the plan writes a single-use **slot recreation authorization** bound to the new generation and the ownership record. Only the start of that generation can use it, before creating the slot. A start interrupted after taking it finishes the creation; one interrupted after creating the slot keeps exactly that slot. The audit records the authorization and, after the start, the recreation;
  - foreign, ambiguous, active, or with a missing or changed ownership record: refused (manual repair).

**Refused:** `snapshot.mode: never` (change the mode first); snapshot state that cannot be interpreted; snapshot-chain positions without their snapshot state; a generation of another source lineage; checkpoints that are malformed or not provably this source's; a generation that already produced all its rows (resume instead: it completes or is replaced).

## `pg-adopt-timeline`

For a PostgreSQL source stopped with `pg_continuity_unproven`, class `timeline_unrecorded`: its checkpoints predate continuity records and the server has switched timeline since. Adoption records the server's current timeline as the start of the source's continuity chain, proven at the checkpoint position F.

**It never moves a checkpoint.** It creates the continuity record once (transition 0, proven at F). At the next start, the ordinary continuity proof runs against that record, and every checkpoint is stamped into the chain at its existing position before the first change is consumed.

**Prerequisites**, all observed on one replication session of the kind the stream itself uses:
- the endpoint is the primary, not a standby;
- the server's system identifier and database OID are the source's recorded lineage;
- the slot exists, is a persistent logical slot, is not invalidated or lost, and is not in use;
- the slot's restart and confirmed positions are at or before F, and the server's WAL reaches F;
- every sink has a checkpoint; each is a stream position without a continuity stamp, and they can be ordered. F is the earliest of them, with no sink left out;
- the source has no continuity record yet.

Anything else is refused and nothing is written. At apply, the same session observations are made again and must be exactly the planned ones.

**Consequence:** you assert that the server history from F to now is this timeline's. A rewind on the same timeline cannot be detected and is unsupported.

## Manual repair

Some states are never changed by a recovery operation, because nothing proves what they mean: a corrupt or unknown-format snapshot or recovery record, snapshot positions without their snapshot state, foreign-lineage state, a replication slot DeltaForge cannot prove it owns, a diverged operation whose state cannot be brought back. Operations refuse these with `manual_repair` (exit 5) and leave them untouched. Repair them from evidence: the incident's evidence, `diagnose`, and the audit trail. Do not delete records from the state store to get past a refusal.

## How identifiers in a plan are made

Some operations create identifiers, for example a new snapshot chain or a continuity chain. These are derived, not random, so recomputing a plan gives the same result:

1. The plan is built with each such identifier set to a fixed placeholder.
2. The **plan seed** is the SHA-256, under its own domain, of that canonical plan: every binding (source identity, lineage, checkpoints and their digests, observed server facts, incident), step and consequence.
3. Each identifier is 128 bits derived from the seed under a separate domain and a purpose (`snapshot_chain`, `continuity_chain`).
4. The identifiers go into the final plan, and the **proof** is the SHA-256 of that final plan.

The proof is never an input to what it proves.

## Audit

Every apply is audited per pipeline: the operation, proof, asserted actor, how the request authenticated, the peer address, the reason, the incidents it resolved, and each step's recorded outcome. Each incident it resolves records the same fields in its own audit trail, in one transition. Later effects of an operation are audited too (for example `slot_recreated` when the authorized start created the slot).

**The actor is asserted, not verified.** The admin token proves possession of the token, not who is using it; the audit records `actor_verified: false`.

## Not in this release

- Enabling PostgreSQL 17 failover slots on an existing slot (`pg-enable-failover-slot`).
- Replaying a Kafka sink from its held checkpoint after an uncertain transaction (`kafka-replay-from-checkpoint`).
- A new source id is a configuration change, not an operation: configure the new id (it starts with a snapshot).
