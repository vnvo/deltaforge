# Bounded retry of failed MySQL forward-proof captures: proposal for review

Status: revision 2, for review before implementation (2026-10-31). Base: main at 57eff64a. Implementation, stage 2 execution, T1a and rc.1 remain held.

## 0. Changes from revision 1

| Review item | Where |
|---|---|
| Decision: stop before the DDL when retries are exhausted | §3.6 |
| 1. A proof-wide deadline over every attempt and backoff, with the time left checked before each capture; cancellation and cleanup at expiry; an in-flight commit resolves through its marker; no hard completion claim while it is outstanding | §3.4, §3.5, invariant 2 |
| 2. Recovery is not the same as deferral: proven, deferred and retrying tables tracked separately; the incident outcome for mixed results; a test of the transition | §3.2, §3.8, tests 5 and 6 |
| 3. Deadline exhaustion tested apart from recovery at the last permitted attempt; checks for no missing events and no duplicate durable bindings; duplicate delivery judged against the existing at-least-once guarantee | §5, tests 2, 3 and 11 |
| Limits accepted as initial constants; tests inject limits per run, with no shared mutable statics | §3.4 |
| A different terminal stop withdraws the retry as `operation_cancelled`; the cause goes in existing fields; no store-format change | §3.8 |
| Error typing approved, with transport failures retried explicitly and authentication, authorization and protocol failures classified separately | §3.3 |

## 1. Problem

When the stream reads a DDL on a tracked table, the source writes the table's pending `ddl` record. Then, before the DDL event is emitted, it proves the post-DDL shape ([mysql_forward_proof.rs](../../crates/sources/src/mysql/mysql_forward_proof.rs), steps 1 to 4). Today one attempt decides. A capture or scan that fails for a transient reason (a metadata-lock wait, a dropped connection) leaves the DDL pending, and the stream moves on.

This change improves how transient failures are handled. It does not repair a data-loss path: the existing deferral is safe (§2.2).

## 2. Current behavior (read from the code at 57eff64a)

### 2.1 Outcomes of one forward proof

| Outcome | Source of it | What happens today |
|---|---|---|
| `Cancelled` (the run stopped) | any capture wait | Stops at once. Nothing is decided. The checkpoint stays before the DDL. |
| `OtherServer`, or no identity | capture connect, schema read, scan | The source stops (`SourceError::Lineage`). |
| `Saturated` | control-plane connect | The source stops before the DDL is emitted or checkpointed (`mysql_control_plane_saturated`, auto_retry). The restart replays the DDL. |
| Store failure in `commit_proofs` (conflicts past the bound, oversize, I/O) | the batch | The source stops before the DDL is emitted. |
| Any other capture error | `ProofError::Capture`, `Open`, `Positions` | **Deferred:** the table stays pending, the DDL is emitted and the checkpoint passes it. |
| Unparseable D, or S not at or after D | position checks | Deferred. |
| Scan error (`Open`, `Read`, `NotReached`, `Limit`, `SeekLimit`, `Incident`, `Framing`, ...) | `shared_interval` | Deferred (every table of the proof). |
| `not_covered`, `relevant_ddl` | coverage and scan result | Deferred (that table). |

Two details matter for the retry rule:

- The single-table `capture` folds four failures into one `ProofError::Capture(String)`: a metadata-lock timeout (1205), a deadlock (1213), busy capture slots and a dropped connection. The batched capture already separates contention, as `BatchCaptureError::Contention { code }`.
- `ProofError::Open` covers both a failed connection and a purged binlog (the scanner's `"purged"`).

The capture's budget is fixed at `LOCK_WAIT_SECS` (30 s), and it bounds only the slot wait, the connect and the lock wait. The schema reads, the position reads and the scan have no deadline of their own.

### 2.2 What a deferral leads to

A deferred DDL's later rows are evaluated after it:

1. The row is unproven (`PendingDdl`). A lazy baseline at the row's position E is tried first (`mysql_baseline::establish_at`). If it captures, the scan of (E, S] is clean and the baseline is decisive, the rows are proven. This is sound: E is the table's first row after the DDL.
2. If the lazy baseline fails too, a row with FULL metadata is selected by its signature. A row without FULL metadata stops the source with `no positional proof ... (PendingDdl)`, and the message advises a re-snapshot.

The checkpoint is already past the DDL, so a restart never proves the DDL again. The usual way to reach step 2 is metadata-lock contention that persists, which defeats both proofs. The re-snapshot advice is then wrong for a transient cause.

## 3. Proposal

### 3.1 Retry in place, with the stream paused at the DDL

The forward proof gets a bounded series of attempts inside `prove`, before the DDL event is emitted and before any later event is read.

1. **One attempt** captures the tables that are still retrying, in order. Then it runs one scan of (D, S], where S is the last capture position of the attempt. Then it runs `commit_proofs` for the tables the attempt proves.
2. **Each attempt is a complete proof.** Its captures and its scan are fresh. Nothing from an earlier attempt's capture or scan is reused. Its evidence is checked exactly as a first attempt's would be: the D <= S order, coverage, and relevant statements. A retry never loosens a check.
3. **Identity is fixed once.** The expected server UUID and the lineage are read once, at the start of `prove`, and every attempt verifies against them. A retry never learns the identity again from the server.

### 3.2 Table states

Every table of the proof is in exactly one state:

| State | Entered when | Terminal |
|---|---|---|
| `Retrying { class }` | at the start (class `not_attempted`), and after a retryable failure (§3.3) | no |
| `Proven` | its batch committed in this proof, its ambiguous batch resolved as committed through its marker, or the replay check finds its binding valid | yes |
| `Deferred { reason }` | a non-retryable refusal or failure that today leads to a deferral (§3.3) | yes |

The only transitions are `Retrying` to `Proven`, `Retrying` to `Deferred`, and `Retrying` to `Retrying` (a new class). A table never leaves `Proven` or `Deferred`.

The proof also records **which tables ever had a retryable failure**: the incident's scope. A table that is `Retrying` with class `not_attempted` and has never failed is outside that scope.

The proof ends in one of three ways:

- **Completed:** no table is `Retrying`. The DDL is emitted, and its `Deferred` tables stay pending exactly as a deferral does today.
- **Exhausted:** some table is still `Retrying` when an attempt cannot start (§3.4). The source stops (§3.6).
- **Stopped:** cancelled, or a terminal error such as saturation, another server, an authentication or authorization failure, or a store failure.

### 3.3 Classification

The capture and scan errors become typed, with no string matching. The new types are:

- `ProofError::Contention { code }` for 1205, 1213 and busy slots (code 0, as in the batched capture);
- `ProofError::Open` split into `Transport` and `Purged`;
- a connection error classified by the table below, from the `mysql_async::Error` kind and the server error code.

The classification is one function, `classify(&ProofError) -> Class`. It matches every variant exhaustively, so a new variant does not compile until it is classified.

| Class | Errors | Outcome |
|---|---|---|
| `contention` | 1205 lock wait timeout, 1213 deadlock, lock-holding slots busy | retried |
| `transport` | `Io` errors of kind `ConnectionRefused`, `ConnectionReset`, `ConnectionAborted`, `BrokenPipe`, `TimedOut`, `UnexpectedEof`, `NotConnected`, `HostUnreachable` or `NetworkUnreachable`; `DriverError::ConnectionClosed`; server 1040 (too many connections), 1053 (server shutdown), 1927 (connection killed by another session); scan `Read`; `NotReached` after the scanner's own fallback | retried |
| `authentication` | server 1045, 1251, 1820, 1862, 3118; `DriverError::UnknownAuthPlugin`, `MysqlOldPasswordDisabled`, `CleartextPluginDisabled`, `NoKeyFound`, `NoClientSslFlagFromServer`; any TLS error | **stop before the DDL** as `SourceError::Auth` (cause `source_auth`, operator action); not retried |
| `authorization` | server 1044, 1142, 1143, 1227 | **stop before the DDL** as `SourceError::Auth`; not retried |
| `protocol` | `DriverError::PacketOutOfOrder`, `UnexpectedPacket`, `PacketTooLarge`, `BadCompressedPacketHeader`, `CantParseServerVersion`, `FromValue`, `FromRow` | not retried; deferred as today, as untrusted evidence |
| `identity` | `OtherServer`, no identity | the source stops (`SourceError::Lineage`); unchanged |
| `saturated` | `Saturated` | stop with `mysql_control_plane_saturated`; unchanged |
| `retention` | `Purged` | not retried; deferred as today |
| `evidence` | `Positions`, `Incident`, `UnsupportedEvent`, `Framing`, `Overshoot`, `AnchorMismatch` after fallback | not retried; deferred as today (fail closed: nothing is registered) |
| `bound` | `Limit`, `SeekLimit` | not retried (a later S only widens the interval); deferred as today |
| `absent` | 1146 (no such table), or another schema-read server error | not retried; deferred as today (the stream reads the statement that removed the table next) |
| `decision` | `not_covered`, `relevant_ddl`, S not at or after D, unparseable D | deferred as today |
| `other` | any `Io` kind or server code not listed above | not retried; deferred as today |

Authentication and authorization currently defer. Retrying them would not help, and a deferral leads to the late row-time stop with the wrong advice. They therefore stop at the DDL boundary, like exhaustion. During a credential rotation, the restart uses the rotated DSN. §7, question 1, asks the reviewer to confirm this change.

### 3.4 Limits and the proof-wide deadline

The limits are initial fixed constants:

| Limit | Value |
|---|---|
| Attempts | 4 in total |
| Deadline | 120 s, measured from the start of the first attempt (after the replay check) |
| Backoff before attempts 2, 3 and 4 | 1 s, 4 s, 16 s |
| Smallest useful capture budget | 1 s |

They are carried in one value, `RetryLimits`, which is given to `prove` through the run's capture context. Production uses `RetryLimits::DEFAULT`. Each test builds its own context with its own limits, so no test changes shared mutable state. The capture's lock-wait budget becomes a parameter, as the batched capture's `lock_wait_secs` already is, instead of the `LOCK_WAIT_SECS` constant.

The deadline governs all MySQL work of the proof:

- **Before each capture:** if less than the smallest useful budget is left, the capture does not start. The table stays `Retrying` (class `deadline`), and the attempt ends there.
  - Otherwise the capture's lock-wait budget is the lesser of `LOCK_WAIT_SECS` and the time left.
  - The whole capture future (slot wait, connect, identity check, lock wait, schema and position reads, COMMIT) runs under a timeout of the time left.
- **Before the scan:** if nothing is left, the scan does not start, and the attempt's captured tables stay `Retrying` (class `deadline`). Otherwise the scan runs under a timeout of the time left.
- **Backoff:** the sleep is the lesser of the scheduled backoff and the time left. If less than the smallest useful budget is left after it, no attempt starts.
- **Attempts:** none starts after the fourth.
- **Exhausted** means that at the point an attempt or a capture would start, some table is still `Retrying` and none of the above allows it.

Store work is not cut by the deadline:

- the replay check;
- `commit_proofs`, which runs to resolution under its existing bounds (`BATCH_CONFLICT_RETRIES`, the store's timeouts, and commit-marker resolution of an ambiguous outcome);
- the C3 freshness reads.

A commit starts only after a completed scan whose evidence proves its tables. Once the deadline has passed, the freshness reads are skipped: those tables are invalidated and reload, which is always safe.

**The resulting bound:** no MySQL capture or scan of the proof starts after the deadline, and none runs past it by more than its cancellation cleanup (§3.5). The proof as a whole can still outlast the deadline, by at most:

- the resolution of the one store commit in flight at the deadline;
- the commit-marker resolution, if that commit's outcome was ambiguous.

There is no hard 120-second completion bound while that resolution is outstanding.

### 3.5 Cancellation, and cleanup at expiry

A **stop** (the run's cancellation token) works as follows:

- The backoff sleep, each capture and the scan are each raced against the token. The proof returns `Cancelled`.
- A commit in flight is not raced against the token. It completes or fails as it does today. If the process ends during a commit, the restart's replay check resolves the table: a valid binding means it committed, and otherwise it is proven again.

A **deadline expiry** works as follows:

- **Capture:** the future is dropped. Its `KillOnDrop` guard kills the server session on a detached task, from the cancellation reserve. The lock-holding slot returns only after the kill, or is retired if the kill fails (existing behavior). No session of the proof keeps holding a metadata lock or a slot.
- **Scan:** the request is dropped. The shared scanner treats it as a cancelled waiter: its pin is released, and the round continues only for other waiters (existing behavior).

At either end, the following holds:

- the batches committed by earlier attempts stay committed;
- `Retrying` tables stay pending;
- the DDL is neither emitted nor checkpointed.

### 3.6 Exhaustion: stop before the DDL

On exhaustion the source stops before the DDL event is emitted or checkpointed. `Proven` tables stay committed, `Retrying` and `Deferred` tables stay pending, and the source stops on the incident's operator-action draft (§3.8).

The restart replays the DDL:

1. `record_ddl` finds its record (`Existing`). There is no snapshot and no C3 install.
2. The replay check resolves each table that already has a binding, through `resolve_binding`, with no recapture.
3. Every other table gets a new proof with a new retry budget. A table deferred for a decision (for example `relevant_ddl`) reaches the same decision again, which is deterministic.

The recovery boundary is the checkpoint before the DDL, named in the incident.

### 3.7 Replay and checkpoint during retries

- **Nothing moves forward:** nothing is emitted, the in-memory position does not pass the DDL, and the durable checkpoint stays at or before it.
- **The `ddl` record** is written once, before the first attempt, and never again.
- **Commits** are the same atomic per-table batches as today (C1), with commit-marker resolution. An attempt resolves an earlier ambiguous batch through its marker, never by appending again. Before capturing, it moves any table whose binding the replay check finds valid to `Proven`.
- **C3:** a snapshot taken before the `ddl` record stays valid across attempts. Only the source's own task writes in between, and the freshness reads still check that the table stream gained exactly the `ddl` record and the binding.
- **The retry budget** lives in memory and restarts with the run. A crash loop cannot retry without bound: exhaustion is an operator-action stop, and restarts are supervised.

### 3.8 Incident

Reason code: `mysql_forward_proof_unavailable` (new).

| Field | Value |
|---|---|
| Component | the source |
| Cause | `source_schema` for `contention`; `source_connect` for `transport` and `deadline` |
| Safety | `halted_safe`. Nothing unproven is emitted, and the checkpoint is before the DDL. |
| Discriminator | `boundary` = the DDL position D, bound to the recovery epoch |
| Evidence (existing keys) | `source_id`; `table` (the first table in scope); `blocking_count` (tables in scope still `Retrying`, or `Deferred` for the withdrawn outcomes); `attempts`; `reason_class` (below); `checkpoint_position` (before D) |
| Actions | `inspect_logs`. The explanation names `performance_schema.metadata_locks` for contention. |

**Lifecycle.** The incident is recorded as `auto_retry` (`record_retrying`) at the first retryable failure; `reason_class` is the failure's class. Failure to record it never fails the proof. The outcome is decided once, when the proof ends:

| End | Tables in scope | Incident |
|---|---|---|
| Completed | every one `Proven` | resolved, `verified_recovery` with check `forward_proof_bound` |
| Completed | at least one `Deferred` | withdrawn, `operation_cancelled`; `reason_class` = `ended_deferred`; `blocking_count` = the deferred count. Not a recovery. The DDL is emitted with those tables pending, as a deferral is today. |
| Exhausted | at least one still `Retrying` | the source stops on the same draft as `operator_action`. The supervisor reclassifies the same incident (audited `reclassified`), with the last retry class and the count of tables still retrying. |
| Stopped: cancelled | any | withdrawn, `operation_cancelled`; `reason_class` = `ended_cancelled` |
| Stopped: another terminal cause | any | withdrawn, `operation_cancelled`; `reason_class` = `ended_by_<reason>`, for example `ended_by_mysql_control_plane_saturated`, `ended_by_lineage` or `ended_by_auth`. The terminal stop raises or carries its own incident, as today. A structured log line names both incident ids. |

Tables deferred without any retryable failure are outside the incident's scope. They are handled exactly as today, with no incident, so a mixed proof can still resolve as `verified_recovery` for the tables that did fail transiently. If no table ever has a retryable failure, no incident is recorded.

**Recording the cause on withdrawal.** The record's `evidence` is an existing field, and today `cancel_auto_retry` leaves it unchanged. The change is one store method parameter: `cancel_auto_retry(id, evidence)` replaces the evidence in the same `Resolved { by: OperationCancelled }` transition. This needs no new resolution, no new evidence key and no format change. Existing callers pass the record's current evidence.

Metrics:

- `deltaforge_mysql_forward_proof_retries_total{class}`;
- `deltaforge_mysql_forward_proof_outcomes_total{outcome}`, with `recovered`, `ended_deferred`, `exhausted`, `cancelled` and `ended_by_terminal`.

### 3.9 What does not change

- Saturation, server identity, store failures and cancellation are never retried in place.
- The evidence checks, the scanner's single fallback, the atomic batches and the C3 freshness checks are unchanged.
- The row-time lazy baseline, FULL selection, and the activation window with its `mysql_capture_contention` are unchanged.
- A DROP gets no forward proof.

## 4. Invariants

1. A retry never emits, checkpoints or registers anything that the same attempt would not prove as a first attempt.
2. **Deadline:** no capture or scan of a proof starts after its deadline or after its fourth attempt, and none runs past the deadline by more than its cancellation cleanup. Completion can exceed the deadline only by the resolution of a store commit already in flight, including its commit-marker resolution.
3. Identity is fixed at the start of `prove`. Any attempt that reaches another server, or cannot establish identity, stops the source without a retry.
4. Invalid or untrusted evidence, deterministic bounds, evidence decisions and protocol failures are never retried. Authentication and authorization failures stop at the DDL boundary.
5. A table is in exactly one of `Retrying`, `Proven` or `Deferred`. Only `Retrying` changes. Across attempts and replays a table is bound at most once.
6. `verified_recovery` is recorded only when every table that had a retryable failure ends `Proven`.
7. On exhaustion, cancellation or any terminal stop, the DDL is neither emitted nor checkpointed, and the restart replays it.
8. One incident per proof boundary and recovery epoch. It is resolved, reclassified or withdrawn, and never left open as `auto_retry` after the proof ends.

## 5. Tests (each written and seen failing first)

**Setup.** The tests are live on MySQL 8.4 unless marked otherwise. Each test builds its run with its own `RetryLimits` (for example a 6 s deadline, 2 s lock waits, backoff 0.2/0.4/0.8 s). The blocker is a second session that holds the table's metadata lock (`LOCK TABLES t WRITE`) and releases it on a signal.

**Delivery checks, wherever events are checked:**

- **No missing events:** every source row and the DDL reach the sink. This is checked with the ledger's equality.
- **No duplicate durable bindings:** at most one `Observed` binding per `ddl` record, and at most one registered version per captured shape. This is read from the store.
- **Duplicate deliveries** are not a failure across a stop and replay: delivery is at-least-once, and consumers deduplicate on the event id. Within one uninterrupted run, the DDL event is delivered once.

| # | Test | Expect |
|---|---|---|
| 1 | Recovers on attempt 2: the blocker is released during the first backoff | Proven on attempt 2; one binding; the DDL is emitted once; the incident is recorded, then `verified_recovery`; the next rows are proven by the forward proof, not by a lazy baseline |
| 2 | Recovers at the last permitted attempt: the last attempt the start rule permits has its start time computed from the injected limits, and the blocker is released shortly before it | Proven on that attempt; `verified_recovery`; the attempt count equals the limit |
| 3 | Exhausted at the deadline: the blocker is held past it | The source stops before the DDL; the checkpoint is before D; the incident is reclassified to `operator_action` with the attempt count and class; no capture session of the proof is in the processlist after the deadline plus the kill slack; no attempt starts after the deadline. Then release and restart: the DDL is bound once, with no missing events. |
| 4 | Deadline within an attempt: three tables, and the second capture would start with less than the smallest useful budget left | It does not start; tables 2 and 3 stay `Retrying`; exhausted |
| 5 | Transient, then deferred: attempt 1 hits the lock wait; before release, the blocking session runs an `ALTER` on the table, so attempt 2 finds `relevant_ddl` | Ends `Deferred`; no `verified_recovery`; withdrawn as `operation_cancelled` with `ended_deferred`; the DDL is emitted with the table pending; the stream then reads the `ALTER` and proves after it |
| 6 | Mixed: table A fails transiently, then is proven; table B is deferred for `relevant_ddl` with no transient failure | `verified_recovery` (B is outside the scope); B is pending, as today |
| 7 | Multi-table exhaustion: one table blocked, one free | The free table commits in attempt 1, exactly once; after exhaustion and restart it is resolved by its binding with no recapture |
| 8 | Commit in flight at the deadline: a store hook delays the batch past the deadline, then loses its acknowledgement | The batch resolves through its marker after the deadline; one binding; no capture or scan starts after the deadline; the freshness reads are skipped and the table is invalidated |
| 9 | Cancelled during a lock wait, during a scan, and during a backoff | `Cancelled`; the capture session is killed; withdrawn with `ended_cancelled`; the checkpoint is before D; the restart binds the DDL |
| 10 | Terminal stops during the retries: another server (DSN redirected after attempt 1), saturation, authentication (password changed between attempts) | The terminal stop with no further attempt; the retrying incident withdrawn with `ended_by_lineage`, `ended_by_mysql_control_plane_saturated` or `ended_by_auth`; the terminal incident raised as today |
| 11 | Paused stream at the deadline: the blocker is held until exhaustion, then the run restarts. A separate case holds the stream near the deadline and recovers at the last permitted attempt (as test 2). | Both: no missing events; no duplicate durable bindings; duplicates judged only against at-least-once; whether the replication connection survived the pause or reconnected from the checkpoint before D |
| 12 | Not retried: a relevant DDL in (D, S]; a purged interval (a scanner segment fault, Docker-free where the seam allows); a missing table; a protocol error (injected) | One attempt; deferred as today; no incident |
| 13 | Classification (Docker-free): every `ProofError` variant and each listed `Io` kind, driver error and server code through `classify` | The class in §3.3 |
| 14 | Withdrawal evidence (Docker-free, store): `cancel_auto_retry(id, evidence)` | Replaces the evidence in the same transition; leaves operator-action records alone; existing callers keep their evidence |

**Mutants:**

- the attempt bound;
- the per-capture deadline check, and the smallest useful budget;
- the timeout around the capture and around the scan;
- the skipping of freshness reads after the deadline;
- one class flipped each way;
- the `verified_recovery` condition (scope against all tables);
- the reclassification on exhaustion;
- the withdrawal outcomes;
- the reuse of an earlier attempt's capture.

Each must be killed or argued equivalent.

The live forward-proof suite runs in the gate's `sources/lib` command. Test 11 may need a serial lane if its run time requires one.

## 6. Out of scope

- The row-time remediation text for `PendingDdl`. After this change it fires only after an evidence refusal or a non-retryable failure, where a re-snapshot or FULL metadata really is the remedy.
- Retrying in the activation window or the lazy baseline, which have their own bounds and incidents.
- Persisting the retry budget across restarts.
- Making the limits configurable.

## 7. Questions for the reviewer

1. Authentication and authorization failures on a forward-proof connection: stop at the DDL boundary as `SourceError::Auth` (proposed), instead of the deferral they get today?
2. Recording the cause on withdrawal: the one store method parameter in §3.8 (the evidence replaced in the same transition, no format change). Accept, or keep the cause only in the log line and the terminal incident?
