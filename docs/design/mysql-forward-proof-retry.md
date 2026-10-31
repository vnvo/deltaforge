# Bounded retry of failed MySQL forward-proof captures: proposal for review

Status: revision 1, for review before implementation (2026-10-31). Base: main at 57eff64a. Stage 2 execution, T1a and rc.1 remain held.

## 1. Problem

When the stream reads a DDL on a tracked table, the source writes the table's pending `ddl` record. Then it proves the post-DDL shape before the DDL event is emitted ([mysql_forward_proof.rs](../../crates/sources/src/mysql/mysql_forward_proof.rs), steps 1 to 4). Today one attempt decides. A capture or scan that fails for a transient reason (a metadata-lock wait, a dropped connection) leaves the DDL pending, and the stream moves on.

## 2. Current behavior (read from the code at 57eff64a)

### 2.1 Outcomes of one forward proof

| Outcome | Source of it | What happens today |
|---|---|---|
| `Cancelled` (the run stopped) | any capture wait | Stops at once. Nothing is decided. The checkpoint stays before the DDL. |
| `OtherServer`, or no identity | capture connect, schema read, scan | The source stops (`SourceError::Lineage`). |
| `Saturated` | control-plane connect | The source stops before the DDL is emitted or checkpointed (`mysql_control_plane_saturated`, auto_retry). The restart replays the DDL. |
| Store failure in `commit_proofs` (conflicts past the bound, oversize, I/O) | the batch | The source stops before the DDL is emitted. |
| Any other capture error | `ProofError::Capture`, `Open`, `Positions` | **Deferred:** the table stays pending, the DDL is emitted and the checkpoint passes it. |
| Unparseable D, S not at or after D | position checks | Deferred. |
| Scan error (`Open`, `Read`, `NotReached`, `Limit`, `SeekLimit`, `Incident`, `Framing`, ...) | `shared_interval` | Deferred (every table of the proof). |
| `not_covered`, `relevant_ddl` | coverage and scan result | Deferred (that table). |

Two details matter for the retry rule:

- The single-table `capture` folds a metadata-lock timeout (1205), a deadlock (1213), busy capture slots and a dropped connection into one `ProofError::Capture(String)`. The batched capture already separates contention: `BatchCaptureError::Contention { code }`.
- `ProofError::Open` means either a failed connection or a purged binlog (the scanner's `"purged"`). These are different classes.

### 2.2 What a deferral leads to

Once a DDL is deferred, its rows are evaluated after it:

1. The row is unproven (`PendingDdl`). A lazy baseline at the row's position E is tried first (`mysql_baseline::establish_at`). If it captures, the scan of (E, S] is clean and the baseline is decisive, the rows are proven and nothing stops. This is sound: E is the table's first row after the DDL.
2. If the lazy baseline fails too, a FULL-metadata row is selected by signature. A row without FULL metadata stops the source with `no positional proof ... (PendingDdl)`. That message advises a re-snapshot.

The checkpoint is already past the DDL, so a restart never proves the DDL again. Only a later row can.

The follow-up recorded on 2026-10-29 said that every deferral stops at the next rows. It is narrower than that: rows stop only when the row-time lazy proof fails too. The common case is persistent metadata-lock contention, which makes both the forward proof and the lazy proof time out. The advice to re-snapshot is then wrong for a transient cause.

## 3. Proposal

### 3.1 Retry in place, with the stream paused at the DDL

The forward proof gets a bounded number of attempts inside `prove`, before the DDL event is emitted and before any later event is read:

1. **One attempt:** capture every table of the open set, then one scan of (D, S], with S the last capture position of the attempt. Then `commit_proofs` runs for the tables that attempt proves.
2. **After each attempt:**
   - A proven table is committed in that attempt and leaves the open set. Its batch is atomic, and replay resolves it through its binding.
   - A table refused for a non-retryable reason leaves the set and is deferred exactly as today (§3.2).
   - A table that failed for a retryable reason stays in the open set.
3. **Next attempt:** after a backoff, the open set is captured again with fresh captures, a fresh S and a fresh scan of (D, S']. Nothing from an earlier attempt's capture or scan is reused.
4. **When the open set is empty:** the proof returns. The C3 results (`Fresh`) of every attempt are returned together.

Each attempt is a complete proof. Its evidence is checked exactly as a first attempt's is, including the D <= S order, coverage and the relevant-statement check. A retry never loosens a check.

The expected server UUID and the lineage are read once, at the start of `prove`, and every attempt verifies against them. A retry never re-learns the identity from the server.

### 3.2 Which failures are retried

| Class | Retried | Why |
|---|---|---|
| Capture lock-wait timeout (1205) or deadlock (1213) on the table's metadata lock | yes | Another session holds or queues the lock, and it can clear. |
| All lock-holding capture slots busy within the lock-wait budget | yes | This is local capacity, and it frees up. Saturation of the control plane is a separate class (below). |
| Capture or scan connection failure (connect, I/O, driver) | yes | Transient. |
| Scan read failure (`Read`), or the end position not reached (`NotReached`) after the scanner's own fallback | yes | The stream failed. Not a statement about the interval. |
| `Saturated` | no: unchanged | Stops at once with `mysql_control_plane_saturated`, as since 5d. |
| `Cancelled` | no: unchanged | Stops at once (§3.5). |
| `OtherServer` or no identity, at any attempt | no: unchanged | The source stops (`SourceError::Lineage`). Fail closed. |
| Purged binlog, or retention loss | no | Repeating cannot restore the interval. Deferred as today. |
| `Positions` (unparseable, or the position went backwards), `Incident`, `UnsupportedEvent`, `Framing`, `Overshoot`, `AnchorMismatch` after fallback | no | Invalid or untrusted evidence. Deferred as today (fail closed: nothing is registered). |
| `Limit`, `SeekLimit` | no | A deterministic bound. A later S only widens the interval. Deferred as today. |
| The table does not exist (1146), or another schema-read error | no | The stream reads the statement that removed it next. Deferred as today. |
| `not_covered`, `relevant_ddl`, S not at or after D | no | A decision about the evidence, not a failure. Deferred as today. |
| Store failure in `commit_proofs` | no: unchanged | Stops before the DDL is emitted, as today. |

To make the table enforceable, the capture errors become typed, with no string matching:

- `ProofError::Contention { code }` for 1205, 1213 and busy slots (code 0, as in the batched capture);
- `ProofError::Open` splits into a connection failure and `Purged`.

The classification is one function, `retryable(&ProofError) -> Option<RetryClass>`, matched exhaustively. A new variant does not compile until it is classified.

### 3.3 Limits

| Limit | Value | Note |
|---|---|---|
| Attempts | 4 in total (1 + 3 retries) | Per proof, per run. |
| Deadline | 120 s from the start of the first attempt | Matches the 2-minute window of PostgreSQL continuity retries. |
| Backoff | 1 s, 4 s, 16 s, capped at the time left | No jitter. One source has one proof at a time. |
| Start rule | An attempt starts only if a full lock-wait budget (`LOCK_WAIT_SECS`, 30 s) still fits before the deadline | No attempt is cut short by the deadline. |

- **Worst case per DDL:** at most 120 s plus one scan, within the scanner's existing bounds. With 30 s lock waits, that is 3 attempts.
- **Configuration:** the values are constants, like `LOCK_WAIT_SECS`. Tests override them through `#[cfg(test)]` statics, as `kill_delay_ms` does. They are not configuration.

The stream stays paused for up to the deadline. If the replication connection drops meanwhile, the existing reconnect resumes from the committed checkpoint, which is before the DDL (§3.6). A live test must show exactly that (§5, test 9).

### 3.4 What happens when the retries are exhausted

**Recommended: stop before the DDL is emitted or checkpointed.** This mirrors saturation:

- every table proven in earlier attempts stays committed;
- the still-open tables stay pending;
- the source stops on the incident in §3.7.

The restart replays the DDL:

1. `record_ddl` finds its record (`Existing`), so there is no snapshot and no C3 install.
2. The replay check resolves each committed table's binding through `resolve_binding`, with no recapture.
3. The remaining tables get a new forward proof with a new retry budget.

This removes the path that led to the misleading re-snapshot advice for a transient cause.

The alternative is to keep deferring after exhaustion, as today, with the incident raised as a warning instead of a stop. That keeps the stream moving when no rows follow, or when the row-time lazy proof or FULL selection would succeed. However, it brings back the late stop with the wrong advice when the contention persists. The reviewer is asked to choose (§7, question 1).

### 3.5 Cancellation

- **Every wait is cancellable:** the backoff sleep, each capture and each scan select on the run's stop token (`CaptureCtx.cancel`).
  - A capture dropped on a stop is killed on the server, as today (`KillOnDrop`).
  - The scan is awaited under the same select. A dropped scan request leaves the shared scanner, as a cancelled waiter does today.
- **A stop returns `Cancelled`:**
  - nothing more is decided;
  - batches committed by earlier attempts stay committed;
  - the open tables stay pending;
  - the DDL is neither emitted nor checkpointed;
  - the restart replays it as in §3.4.
- **Incident:** a retrying incident is withdrawn (`cancel_retrying`, resolution `operation_cancelled`). An intentional stop leaves nothing blocking.

### 3.6 Replay and checkpoint

- **During retries:**
  - nothing is emitted;
  - the in-memory position does not advance past the DDL;
  - the durable checkpoint stays at or before it.
  - The `ddl` record is written once, before the first attempt, and is never rewritten.
- **Across attempts:** each attempt's commit is the same atomic per-table batch as today (C1), with the same commit marker resolution. If an attempt's batch ended ambiguously, the next attempt resolves it through its marker, not by appending again. Before capturing, the next attempt drops any table that the replay check (`resolve_binding`) finds already bound.
- **After a crash, at any point:** the restart replays the DDL, and replay works as in §3.4. The retry budget lives in memory, so it restarts with the run. A crash loop cannot retry without bound by itself: exhaustion is an operator-action stop, and restarts are supervised.
- **C3:** a snapshot taken before the `ddl` record stays valid across attempts. Only the source's own task writes in between, and the freshness reads still check that the table stream gained exactly the `ddl` record and the binding. A table committed in attempt k is checked after attempt k, as today.

### 3.7 Incident

Reason code: `mysql_forward_proof_unavailable` (new).

| Field | Value |
|---|---|
| Component | the source |
| Cause | `source_schema` for contention; `source_connect` for connection and read classes |
| Safety | `halted_safe`. Nothing unproven is emitted, and the checkpoint is before the DDL. |
| Discriminator | `boundary` = the DDL position D. It is epoch-bound when recorded as retrying. |
| Evidence | source id; first table and table count; class (`lock_wait_timeout`, `deadlock`, `capture_slots_busy`, `connect`, `read`, `not_reached`); attempts; elapsed ms; checkpoint position (before D); DDL position |
| Actions | `inspect_logs`. The explanation names `performance_schema.metadata_locks` for the contention classes. |

Its lifecycle reuses the PostgreSQL continuity pattern (I1c):

1. **First retryable failure:** recorded as `auto_retry` (`record_retrying`), bound to the recovery epoch. It does not fail the proof if recording fails.
2. **A later attempt proves every open table:** resolved as `verified_recovery` (check `forward_proof_bound`).
3. **Exhausted:** the source stops on the same draft as `operator_action`, and the supervisor reclassifies the same incident (audited `reclassified`).
4. **Cancelled:** withdrawn as `operation_cancelled`.
5. **A different stop during the retries** (saturation, another server, a store failure): that stop raises its own incident. The retrying incident is withdrawn so that it does not stay open as `auto_retry` for a condition no longer retried. §7, question 3, asks which resolution to use.

Metrics: `deltaforge_mysql_forward_proof_retries_total{class}` and `deltaforge_mysql_forward_proof_retry_exhausted_total`.

### 3.8 What does not change

- Saturation, server identity, store failures and cancellation behave as they do today. They are never retried in place.
- The evidence checks, the scanner's own single fallback, the atomic batches and the C3 freshness reads are unchanged.
- The row-time lazy baseline, FULL selection, and the activation window's three batched captures with `mysql_capture_contention` are unchanged.
- A DROP still gets no forward proof.

## 4. Invariants

1. A retry never emits, checkpoints or registers anything that the same attempt would not prove as a first attempt.
2. At most 4 attempts and 120 s per proof per run, plus one bounded scan. Every wait is cancellable.
3. Identity is fixed at the start of `prove`. Any attempt that reaches another server, or cannot establish identity, stops the source with no retry.
4. Invalid or untrusted evidence, deterministic bounds and evidence decisions are never retried. They are deferred as today.
5. Each attempt commits per-table atomic batches. A table is bound at most once, across attempts and replays.
6. On exhaustion, cancellation or a stop, the DDL is neither emitted nor checkpointed, and the restart replays it. (If the reviewer chooses the alternative in §3.4, exhaustion defers instead.)
7. One incident per proof boundary and recovery epoch. It is resolved, reclassified or withdrawn, never left open as `auto_retry` after the source stops.

## 5. Tests (each written and seen failing first)

Live tests on MySQL 8.4 unless marked otherwise. They shorten the limits through test statics. The blocker is a second session that holds the table's metadata lock (`LOCK TABLES t WRITE`) and releases it on a signal.

1. **Recovers within the bound:**
   - setup: the blocker is held across the first attempt and released before the second;
   - expect: exactly one binding; the DDL is emitted once; the incident is recorded and then resolved (`verified_recovery`); no deferral; the rows after the DDL are proven by the forward proof, not by a lazy baseline.
2. **Exhausted:**
   - setup: the blocker is held past the deadline;
   - expect: the source stops before the DDL is emitted; the checkpoint is before D; the incident is `operator_action` with the attempt count and class; the attempts are bounded in count and wall time.
   - Then release the blocker and restart: the replay binds the DDL with one binding and no duplicate event.
3. **Multi-table:**
   - setup: one table is blocked and the other is not;
   - expect: the free table commits in attempt 1, exactly once.
   - Exhausted, then restarted: the free table is resolved by its binding without a recapture; the blocked one is proven.
4. **Cancelled during a lock wait, and during a backoff:**
   - expect: `Cancelled`; the session is killed; the incident is withdrawn (`operation_cancelled`); the checkpoint is before D; the restart binds the DDL.
5. **Another server between attempts** (the DSN is redirected after attempt 1): the `Lineage` stop, with no further attempt.
6. **Not retried** (one attempt only, deferred as today, with no retrying incident):
   - a relevant DDL in (D, S];
   - a purged interval (a fault on the scanner's segments, Docker-free where the scanner seam allows);
   - a missing table.
7. **Saturation during a retry:** the `mysql_control_plane_saturated` stop; the retrying incident is withdrawn as ruled.
8. **Classification (Docker-free):** every `ProofError` variant through `retryable`. This is the one place a new variant must be classified. It is a behavior table, not a call-sequence check.
9. **Paused stream:**
   - setup: the stream is held at the DDL for the full deadline (blocker held, then released just before the last attempt);
   - expect: the proof succeeds; after it, either the stream continues or it reconnects from the checkpoint before D. Either way the ledger shows no lost and no duplicate row or DDL.
10. **Mutants:** the attempt bound, the start rule, the class table (one class flipped each way), the reclassification on exhaustion, the withdrawal on cancel, and the reuse of an earlier attempt's capture. Each must be killed or argued equivalent.

The live forward-proof suite runs in the gate's `sources/lib` command. No new gate lane is needed unless test 9's run time requires one.

## 6. Out of scope

- The row-time remediation text for `PendingDdl`. After this change it fires only after an evidence refusal or a non-retryable failure, where a re-snapshot or FULL metadata really is the remedy. Rewording it is a separate, small change, if wanted.
- Retrying in the activation window or the lazy baseline, which already have their own bounds and incidents.
- Persisting the retry budget across restarts.

## 7. Questions for the reviewer

1. After exhaustion: stop before the DDL (recommended, §3.4), or keep deferring and raise the incident as a warning?
2. Limits: 4 attempts, 120 s, backoff 1/4/16 s, constants rather than configuration. Accept, or set other values?
3. The retrying incident when a different stop ends the retries: withdraw it as `operation_cancelled` (no store change, but the name is imprecise), or add a `superseded` resolution that names the stop's incident (a small store change)?
