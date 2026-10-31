# Bounded retry of failed MySQL forward-proof captures: proposal for review

Status: revision 2, approved for implementation (2026-10-31) with the clarifications of its review folded in (§0, last four rows). Base: main at 57eff64a. Stage 2 execution, T1a and rc.1 remain held.

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
| Approval clarification 1: authentication and authorization stop before the DDL on an operator-action incident; transport failures, including those during TLS negotiation, stay transport; not every TLS error is bad credentials | §3.3, §3.8 |
| Approval clarification 2: the withdrawal keeps the original evidence and updates only the terminal cause and count, in the same conditional transition; existing callers keep evidence with no read-and-replace; operator-action records are left unchanged | §3.8 |
| Approval clarification 3: `commit_proofs` stops between batches and replans at the deadline or a stop, after resolving a batch already started; the remaining overrun is listed | §3.4, §3.5, invariant 2 |
| Approval clarification 4: recovery advice stays precise (a later lazy proof can still succeed); the retry budget is bounded per run only | §3.7, §6 |

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
| `authentication` | server 1045, 1251, 1820, 1862, 3118; `DriverError::UnknownAuthPlugin`, `MysqlOldPasswordDisabled`, `CleartextPluginDisabled`, `NoKeyFound`, `NoClientSslFlagFromServer`; on a scan session, the connector's authentication failure | **stop before the DDL** on the operator-action incident (§3.8, cause `source_auth`); not retried |
| `authorization` | server 1044, 1142, 1143, 1227 | **stop before the DDL** on the operator-action incident (§3.8, cause `source_auth`); not retried |
| `protocol` | `DriverError::PacketOutOfOrder`, `UnexpectedPacket`, `PacketTooLarge`, `BadCompressedPacketHeader`, `CantParseServerVersion`, `FromValue`, `FromRow` | not retried; deferred as today, as untrusted evidence |
| `identity` | `OtherServer`, no identity | the source stops (`SourceError::Lineage`); unchanged |
| `saturated` | `Saturated` | stop with `mysql_control_plane_saturated`; unchanged |
| `retention` | `Purged` | not retried; deferred as today |
| `evidence` | `Positions`, `Incident`, `UnsupportedEvent`, `Framing`, `Overshoot`, `AnchorMismatch` after fallback | not retried; deferred as today (fail closed: nothing is registered) |
| `bound` | `Limit`, `SeekLimit` | not retried (a later S only widens the interval); deferred as today |
| `absent` | 1146 (no such table), or another schema-read server error | not retried; deferred as today (the stream reads the statement that removed the table next) |
| `decision` | `not_covered`, `relevant_ddl`, S not at or after D, unparseable D | deferred as today |
| `other` | any `Io` kind or server code not listed above | not retried; deferred as today |

Authentication and authorization currently defer. Retrying them would not help, and a deferral leads to the late row-time stop. They therefore stop at the DDL boundary, like exhaustion, without a retry and without checkpointing the DDL. During a credential rotation, the restart uses the rotated DSN.

TLS: neither client in this build negotiates TLS (`mysql_async` is built without a TLS feature, and the vendored binlog connector has none), so no TLS error can arise. A transport failure during a handshake is an I/O error and classified by its kind (`transport` above), never as credentials. Enabling a TLS feature adds an error variant the exhaustive match does not cover, so it cannot compile until TLS errors are classified, separating transport failures from certificate and credential failures.

The vendored binlog connector keeps no server error code: its server errors are `ConnectError` text. On scan sessions, its errors are classified once where they arise, with the same predicates the main stream already uses for them (`is_auth_error`, `is_purged_error`), plus the connector's own connect-timeout text as `transport`. Any other `ConnectError` is `other`. Every class is carried typed from there on.

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

Store work under the deadline:

- A commit starts only after a completed scan whose evidence proves its tables.
- `commit_proofs` checks the deadline and the stop before planning each batch and before each replan (after a conflict or a repair), and again after the plan is read: before each repair write and immediately before the atomic commit. Past either, it starts no further batch, replan or repair. It returns what it committed; the rest stay `Retrying` (deadline) or pending (stop).
- A stop that arrives while the last batch resolves is still honored, though every table committed: the bindings stay, the proof ends `Cancelled`, the retrying incident is withdrawn, and the DDL is neither emitted nor checkpointed. The replay finds the bindings. The retry loop also checks the stop before it completes.
- A batch already started is never cut: its `commit_atomic_counted`, with its storage retries (`BATCH_STORAGE_RETRIES`) and the commit-marker resolution of an ambiguous outcome, runs to its result.
- The freshness phase is skipped if the deadline or a stop is observed before it is entered: those tables are invalidated and reload, which is always safe. Once the phase is entered, its reads run under the existing store limits; there is no check before each read.

**The resulting bound:** no MySQL capture or scan of the proof starts after the deadline, and no store batch, replan or repair write starts after it. Store work already in progress at the deadline is not cut, so the proof can still outlast the deadline, by:

- planning reads in progress (after them, no batch starts);
- a repair write in progress;
- the store batch in progress: its commit with up to `BATCH_STORAGE_RETRIES` storage retries and the commit-marker resolution;
- replay reads in progress (the check for an existing binding at the start of an attempt);
- freshness reads, once their phase is entered before the deadline or a stop is observed;
- the incident store write that records the outcome (§3.8);
- in the background, after the proof has returned: the server-side kill of a dropped capture session (the existing detached kill, bounded by its own connect and statement), and the end of a dropped scan session (its abortive close makes the server's next write fail).

None of these is bounded by the deadline itself, so there is no hard 120-second completion claim. Each is bounded by its own existing limits.

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
- **The retry budget** lives in memory and is bounded per run only. Each run that replays the DDL gets a new budget. Nothing in this change bounds attempts across repeated restarts: how often a stopped source is restarted is decided outside it.

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
| Stopped: authentication or authorization | any | the source stops on this incident's draft as `operator_action`, with class `authentication` or `authorization` and cause `source_auth`. A retrying incident of the same boundary and epoch is the same incident, so the supervisor reclassifies it, as on exhaustion. |
| Stopped: another terminal cause | any | withdrawn, `operation_cancelled`; `reason_class` = `ended_by_<reason>`, for example `ended_by_mysql_control_plane_saturated`, `ended_by_lineage` or `ended_by_store`. The terminal stop raises or carries its own incident, as today. A structured log line names both incident ids. |

Tables deferred without any retryable failure are outside the incident's scope. They are handled exactly as today, with no incident, so a mixed proof can still resolve as `verified_recovery` for the tables that did fail transiently. If no table ever has a retryable failure, no incident is recorded.

**Recording the cause on withdrawal.** The record's `evidence` is an existing field, and today `cancel_auto_retry` leaves it unchanged. The change: `cancel_auto_retry_with(id, update)`, where `update` holds only the terminal entries (`reason_class`, `blocking_count`, `attempts`).

- Inside the same conditional transition that resolves the record, it overlays those entries on the record's current evidence. Every other entry, including the original diagnostics, is kept.
- The transition's guard is unchanged: a resolved record or an operator-action record is left as it is, and nothing is written.
- `cancel_auto_retry(id)` becomes `cancel_auto_retry_with(id, empty)`, so existing callers keep the evidence as stored. No caller reads and replaces evidence, so no newer evidence can be overwritten.

This needs no new resolution, no new evidence key and no format change.

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
2. **Deadline:** no capture, scan, store batch, replan or repair write of a proof starts after its deadline or after its fourth attempt, and no capture or scan runs past the deadline by more than its cancellation cleanup. The freshness phase is skipped if the deadline or a stop is observed before it is entered; once entered, its reads use the existing store limits. Store work already in progress (planning, repair, a batch with its commit-marker resolution, replay and freshness reads), the outcome's incident write and the background cleanup (§3.4) can outlast it, each bounded only by its own limits.
3. Identity is fixed at the start of `prove`. Any attempt that reaches another server, or cannot establish identity, stops the source without a retry.
4. Invalid or untrusted evidence, deterministic bounds, evidence decisions and protocol failures are never retried. Authentication and authorization failures stop at the DDL boundary.
5. A table is in exactly one of `Retrying`, `Proven` or `Deferred`. Only `Retrying` changes. Across attempts and replays a table is bound at most once.
6. `verified_recovery` is recorded only when every table that had a retryable failure ends `Proven`.
7. On exhaustion, cancellation or any terminal stop, the DDL is neither emitted nor checkpointed, and the restart replays it.
8. One incident per proof boundary and recovery epoch. It is resolved, reclassified or withdrawn, and never left open as `auto_retry` after the proof ends.

## 5. Tests

The live regressions (1 and 2 below) were written first and seen failing on the code before this change; so were the four planning and last-batch regressions, on 44d5958c and d6d50ea7 (three proofs succeeded after the deadline or stop, and a repair wrote after the stop): the transient lock wait deferred the DDL instead of binding it, and the held lock let the DDL be emitted instead of stopping the source.

**Docker-free** (`mysql_forward_proof::tests::store::retry`). These run the real retry loop (`prove_with`), `commit_proofs`, the store and the incident store, on a paused clock. Only the MySQL capture and scan are scripted (the `Probe` seam; the live probe calls the real `capture` and the shared scanner).

| Test | Shows |
|---|---|
| `the_last_permitted_attempt_can_recover` | With the production bounds, attempts start at 0, 31, 65 and 111 s; the fourth proves the table; `verified_recovery` (`forward_proof_bound`) |
| `four_failed_attempts_exhaust_the_proof` | Four attempts, nothing past 120 s; the operator-action draft is the retrying incident's identity; nothing bound |
| `no_fifth_attempt_with_time_left` | With an hour's deadline, four failed attempts and no fifth: the attempt bound holds with time to spare |
| `no_attempt_starts_after_the_deadline` | With 100 s, the fourth attempt (111 s) never starts: the backoff is cut to the 5 s left, then exhaustion after three |
| `a_capture_needs_its_smallest_budget_left` | Within an attempt, tables 2 and 3 are not captured with 0.5 s left; they stay retrying (`deadline`) |
| `a_scan_is_cut_at_the_deadline` | A scan still running at the deadline is dropped; the table stays retrying; exhausted at exactly 10 s |
| `a_transient_failure_then_a_refusal_is_not_a_recovery` | Contention, then untrusted evidence: deferred; withdrawn as `ended_deferred`, never `verified_recovery`; the original evidence kept |
| `a_deferral_outside_the_scope_leaves_a_recovery` | Mixed: one table recovered, another deferred with no transient failure (outside the scope): `verified_recovery` |
| `a_free_table_commits_once_while_another_retries` | Multi-table: the free table commits once; after exhaustion the restart resolves it by its binding (not captured, no second version) and proves the other |
| `no_batch_starts_after_the_deadline` | Expiry between batches: the first batch, started before the deadline, resolves after it; no second batch starts; the second table stays retrying |
| `a_stop_during_commit_resolution_starts_no_further_batch` | A stop while the first batch's lost acknowledgement is resolved through its marker: one binding, no further batch or replan, `Cancelled`, withdrawn as `ended_cancelled` |
| `no_batch_starts_when_the_deadline_passes_while_planning` | The deadline passes while the commit's plan is read: no write; nothing registered or bound; exhausted |
| `no_batch_starts_after_a_stop_while_planning` | A stop while the plan is read: no write; `Cancelled` |
| `no_repair_starts_after_a_stop_while_planning` | A plan that needs a repair (an entry without its indexes), stopped while read: no repair write |
| `a_stop_during_the_last_batch_is_honored` | One batch, stopped while its lost acknowledgement is resolved: the binding stays, `Cancelled`, `ended_cancelled`; the replay captures nothing and keeps one binding |
| `freshness_is_skipped_past_the_deadline` | A batch that resolves after the deadline still binds the table, but the freshness phase is skipped and its timeline is not offered for install (it reloads); before the deadline the same proof offers it |
| `a_stop_during_the_freshness_reads_is_honored` | A stop after the last batch committed, while the freshness phase reads: the binding stays and the proof ends `Cancelled`; the DDL is not emitted |
| `a_stop_ends_the_retries` | A stop in a backoff and in a lock wait ends the proof at that instant; `ended_cancelled`; nothing bound |
| `an_authentication_failure_stops_without_a_retry` | After a transient failure: the operator-action draft of the same incident, cause `source_auth`, no further attempt |
| `an_authorization_failure_stops_at_once` | No retry and no retrying incident |
| `another_server_stops_the_retries` | Lineage stop, no further attempt, `ended_by_lineage` |
| `saturation_stops_the_retries` | `mysql_control_plane_saturated` as before, `ended_by_mysql_control_plane_saturated` |
| `every_proof_error_is_decided` | Each `ProofError` and failure class decided as in §3.3 |

Also Docker-free: `mysql_failure::tests` (server codes, I/O kinds, driver errors, the binlog connector's errors, and the vendored connect-timeout text) and `a_withdrawal_updates_only_its_terminal_evidence` in the incident store (the overlay in the same transition, existing callers' evidence unchanged, operator-action records untouched).

**Live** (MySQL 8.4; per-run injected bounds of 2 s lock waits within 8 s; the blocker is a session holding `LOCK TABLES t WRITE` with the DDL run under it):

| # | Test | Shows |
|---|---|---|
| 1 | `a_transient_lock_wait_is_retried_and_the_ddl_bound` | Released during the first backoff: one durable binding, one new version, no lazy baseline, the row after the DDL decoded with the new column, `verified_recovery` |
| 2 | `retries_exhausted_stop_the_source_before_the_ddl` | Held past the deadline: stops before the DDL within the bound, at most four attempts, no capture session left waiting, checkpoint unchanged, nothing of the table emitted, the retrying incident still `auto_retry` with the stop draft's identity. Restart: one binding, the row after the DDL delivered, and once within the run |
| 3 | `a_transient_failure_then_a_refusal_is_a_deferral` | The blocking session changes the table again before releasing it: deferred, withdrawn as `ended_deferred`, the second DDL proven |
| 4 | `a_stop_during_a_lock_wait_ends_the_proof` | Stopped in the second attempt's lock wait: ends within 3 s, the session killed, nothing emitted, checkpointed or bound, `ended_cancelled`; the restart binds |
| 5 | `an_authorization_failure_stops_before_the_ddl` | `SELECT` revoked: stops at once on the operator-action draft (`authorization`, `source_auth`, root error `Auth`), no retrying incident, no credentials in the error; with the grant back the restart binds |

Exact timing (the last permitted attempt, the deadline boundary) is tested Docker-free on the paused clock; live tests assert bounds only, because a capture's MySQL `lock_wait_timeout` is whole seconds of what is left after its connect.

**Delivery checks:** the live tests check that the rows after the DDL reach the sink (no missing event), that a DDL has exactly one durable binding, and that a row is delivered once within an uninterrupted run. A duplicate across a stop and replay is not a failure (delivery is at-least-once).

**Not live:** a multi-table DDL with only one table blocked cannot be produced reliably (a `RENAME` under `LOCK TABLES` locks all of its tables), so that case is Docker-free. Another server, saturation and authentication during the retries are Docker-free; the live suite already covers each stop on its own.

**Mutants** are listed in the PR with their results.

## 6. Out of scope

- The row-time remediation text for `PendingDdl`. After this change, a deferral follows only an evidence refusal or a non-retryable failure. The row-time lazy proof can still succeed for the rows after it, so a deferral does not by itself mean a re-snapshot or FULL metadata is needed; the text is left as it is here.
- Retrying in the activation window or the lazy baseline, which have their own bounds and incidents.
- Persisting the retry budget across restarts.
- Making the limits configurable.
