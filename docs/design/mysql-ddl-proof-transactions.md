# MySQL forward-proof transactions on PostgreSQL: proposal for review

Status: revision 2, approved for implementation within C1 to C3 (2026-10-30), with the clarifications of the second review folded in (§0). Stage 2, T1a and rc.1 remain held.

## 0. Changes from revision 1

| Review correction | Where |
|---|---|
| 1. Cancellation and saturation leave the durable pending record pending; they do not "write nothing" | §4 invariants 7, 8 |
| 2. Multi-table atomicity: one batch is atomic, several batches are not; budget overflow and an oversized single table | §3 C1 (batch boundaries), §4 invariants 3, 9, 11 |
| 3. C3 coherence: how appends and barriers are detected, where sequences come from, versions, cache bounds, the caller's invalidation | §3 C3, §4 invariant 10, §5 cache races |
| 4. Replay keeps `resolve_binding`'s full evidence check | §4 invariant 6 |
| 5. Reproducible instrumented builds of every revision, with an overhead check | §6 |
| Acceptance clarification and harness retention | §7, §8 |
| Second review: freshness holds at the check, not through the install; the writer-ownership guarantee; C3 off until it holds | §3 C3 (ownership), §4 invariant 10, §5 |
| Second review: bounded preflight memory; sizes rechecked on re-plan | §3 C1 (preflight) |
| Second review: variation rule only on end-to-end metrics, absolute tolerances, inconclusive stays inconclusive, histograms are distributions | §6, §7 |
| Second review: an authoritative drain (durable sink frontiers, ledger equality, bounded timeout) | §7 |
| Second review: verify the durable copy after each series | §8 |

## 1. Problem

Since the mass-invalidation work (#178 to #188), each tracked DDL costs more PostgreSQL store work than before.

| Measure per DDL (one CREATE TABLE, PostgreSQL statement log) | Before (f0171b4e) | Now (3ff3773c) |
|---|---|---|
| Store statements | 41 | 61 |
| Commits | 3 | 11 |
| Store-lock acquisitions | 3 | 8 |

Measured cost (interleaved A/B, all DDL rows delivered; the evidence for these runs is limited, see the report's README):

| Test | PostgreSQL now / before | SQLite now / before |
|---|---|---|
| 300 DDLs, no load | 53.5 / 51.3 ms (+4%) | 22.7 / 22.5 ms |
| 1,000 DDLs under 2,000 rows/s | 87.5 / 77.6 ms (+13%) | 39.7 / 46.8 ms |
| 4,200-DDL phase of the full run | 79-95 / 43 ms (about 2x) | 41 / 44 ms |

The statement count explains the unloaded difference. It does not yet explain the loaded or burst gap (§6).

## 2. Where the transactions come from

The DDL path (`mysql_event.rs` DDL handling, then `mysql_forward_proof::prove`) writes in this order:

| Step | Code | Before | Now |
|---|---|---|---|
| 1. Pending DDL record at D | `mysql_activation::record_ddl` (`log_append_if_absent`) | 1 locked txn | 1 locked txn |
| 2. Replay check | `read_table` + `resolve_binding` | reads | reads |
| 3. Capture and scan of (D, S] | `capture`, `mysql_proof_scanner::shared_interval` | no store writes | no store writes |
| 4. Registration of the captured shape | `register_captured` -> `DurableSchemaRegistry::register_with_checkpoint` (serial path) | | |
| 4a. stream marker, numbering floor, version index, hash index | `kv_put` x4 (guarded namespace) | 4 unlocked statements | 4 locked txns (#178) |
| 4b. schema entry | `log_append` | 1 locked txn | 1 locked txn |
| 4c. sequence high-water | `bump_hw` | 1 unlocked statement | 1 atomic batch with commit marker (#178, #179) |
| 5. Observed binding at D | `record_forward_proof` (`validate_version` read, `log_append_if_absent`) | 1 locked txn | 1 locked txn |
| 6. History reload on the table's next row | the caller invalidates every touched timeline after `prove` (`mysql_event.rs`); the 5e loader reloads | per-table reads | 3 read-only snapshot txns |

Steps 4a and 4c became transactions because guarded writes must hold the store lock (#178) and the high-water is a monotonic max decided under it. Those rulings stay. The serial registration path is the problem. Activation windows, snapshot baselines and failover drift already register through `plan_registrations` in one atomic batch; the forward proof is the last hot path still on the serial path.

## 3. Proposed change

### C1. Atomic batches for the forward proof

After the capture and scan checks pass, the proof plans one or more `AtomicBatch`es (lock first, READ COMMITTED, commit marker). Every proven table is wholly inside exactly one batch. For each such table, its batch holds:
- the registration writes from `plan_registrations`: the same numbering (main head, legacy floor, other lineages), the same index writes, and the high-water as `KvMax`;
- the Observed binding as `LogAppendIdempotent` under its forward capture identity (the single-call `log_append_if_absent` rule: an identical stored entry answers with its original sequence; different bytes fail closed);
- the preconditions the plan's decisions rest on (`plan_registrations`' `KvIs` and `LogHead` preconditions; nothing new).

Only after a batch commits does the registry cache learn its versions (`admit_committed` with the committed sequence, as the window does). Steps 4a to 5 (7 write transactions) become 1 per batch. `register_with_checkpoint` stays for its other callers.

**Batch boundaries (review correction 2).** Atomicity holds for exactly one committed `AtomicBatch`: one PostgreSQL transaction. The array statements inside it (`set_chunks`) are chunks of that transaction and stay atomic. Two committed batches are never atomic with each other.
- **Budget.** A batch is filled table by table under the window's cost budget, the same accounting the window path uses (`prefix.offer`). That budget stays inside the store's hard limits: `MAX_BATCH_OPS` (16,384 ops), `MAX_BATCH_RESULT_BYTES` (256 KiB of encoded outcome) and `MAX_STATE_ITEM_BYTES` (16 MiB per item).
- **Overflow.** A multi-table DDL whose proven tables do not fit one batch is committed as several batches, in table order, with each table wholly inside one batch. A crash between them leaves some tables bound and the rest pending. That is the partial state a multi-table DDL can already reach today, where tables are proven one at a time, and replay resolves it per table (invariant 6).
- **Preflight, bounded.** Before the first commit, a sizing pass walks the proven tables one at a time. It keeps only one cost number per table, computed with the window's cost function from that table's capture and plan reads. It does not build or keep a multi-table plan. The plan, with its encoded ops, exists for one batch at a time: built, committed, then dropped before the next batch is planned. The memory the proof holds is therefore:
  - the captures of the DDL's tables, which `prove` already holds today (C1 does not change this);
  - the cost list;
  - one batch's plan, itself within the window's budget.
- **Re-plan after a conflict.** A conflict discards the batch's plan, reads again, and rebuilds the plan for the same tables. The sizes are checked again against the budget and the store limits before the retry commits.
  - If the tables no longer fit together, the rest are split into further batches.
  - If one table alone no longer fits, the source stops with the classified incident. Nothing of that batch is written. Earlier batches of the same DDL stand, leaving the per-table partial state of invariant 11.
- **Oversized single table.** The preflight decides all sizing before the first batch of the proof commits. A table whose own contribution exceeds an empty batch's budget, or which has one item over `MAX_STATE_ITEM_BYTES`, stops the proof before any of its batches commit. The source stops with the classified incident (`mysql_table_too_large` or `state_item_too_large`, as the window does). Nothing of the proof is written; the pending record stays; the checkpoint does not advance; the DDL event is not emitted. A table is never split across batches.

### C2. The pending DDL record stays a separate, earlier write

It must be durable before the capture, so that a failed or stopped proof leaves the DDL pending (rows after it stay unproven). It is not merged into C1. `record_ddl` changes only to return its append outcome (status and sequence), which C3 needs.

### C3. Install committed records instead of re-reading them (review correction 3)

`install_committed` installs unconditionally and checks nothing. The window can call it that way because its batch carries `LogHead` preconditions on the table stream and both barrier streams at the timeline's read heads. C3 does not add those preconditions to C1: an unrelated barrier would then fail the proof's commit. C3 instead checks after the commit and installs only when every condition below holds. Otherwise it invalidates, which is today's behavior.

1. **Snapshot before the pending record.** Before `record_ddl`, the DDL handler takes the cached timeline T of each tracked table (an `Arc`, with its `table_head` h and its barrier heads), if one is cached. No store read.
2. **Sequences come from the writes themselves.**
   - The pending record's sequence `a` comes from `record_ddl`'s append outcome. It must be `Inserted`; `AlreadyPresent` is a replay, so the table is not installed.
   - The binding's sequence `b` comes from the committed batch result (`CommitResult::Committed(seqs)[op]`). After an ambiguous commit resolved from the marker, the sequences come from the marker's encoded outcome (#179).
3. **Table appends.** Two reads check freshness: one batched read of the heads of both barrier streams (one snapshot), and one read of the table stream's entries after h. The table is installed only if the table stream shows exactly `[a, b]` (the existing `appended_alone` rule, extended to two appends) and both barrier heads are where T read them. Any other append, by this source or any other writer, means no install. Each read establishes freshness at its own moment; the time after it, like the time after the check, is covered by writer ownership below. The two entries are decoded from the store exactly as a load decodes them, so the installed records are the stored bytes.
4. **Database and lineage barriers.** A barrier for this same statement is appended before `prove`, and `append_barrier` already calls `barrier_appended`, which removes T. So a statement with a barrier never installs. A barrier from elsewhere is caught by step 3's read. In-process invalidations between steps 1 and 5 (eviction, `barrier_appended`, `invalidate_all`) are caught by identity: the install goes ahead only if the cache still holds the same `Arc` T. Otherwise it skips the install and leaves the entry alone.
5. **Schema versions.** The augmented timeline is built by the read path's own admission, with the planned versions passed as known hashes (`PlannedVersions`, as the window's `admit_decisive_planned` does). Its contents therefore equal what a fresh read would validate, with no registry read. If admission refuses, the table is invalidated.
6. **Cache bounds.** The install goes through `put_timeline`, with the same table and byte accounting and eviction as a loaded timeline. Installing never bypasses `max_tables` or `max_bytes`.
7. **The caller's invalidation changes.** Today `mysql_event.rs` invalidates every touched key after `prove` returns. That would discard C3's install. `prove` will return the set of keys it installed, and the caller invalidates only the touched keys outside that set. The loader's shape-cache invalidation after a DDL is unchanged.

**Writer ownership (second review).** The step 3 read establishes freshness at its snapshot, not through the install. Between the check and `put_timeline`, nothing checks again: a write by another process in that interval would be missed, and `Arc` identity cannot detect it. The interval is safe only because no other writer can exist, which is the guarantee the existing install sites (`install_committed` in the window, `append_admitted`) already rely on:
1. **Across processes, the store gate.** A server acquires the durable store gate (`storage::adapters::store_gate`, role `server`) right after the backend opens, before any registry or pipeline exists. A second server or a migration on the same store fails to start. The gate is broken only by the operator command, after verifying the holder is not alive.
2. **Within the server, unique source ids.** Every activation and barrier stream lives under its `source_id` prefix. The pipeline manager refuses a second pipeline with a source id already in use (the `ACTIVE_SOURCES_NS` slot).
3. **Within the source, one task.** Every write to the source's activation and barrier streams runs on the source's run task, through `&mut` of its context. `append_barrier` is the only barrier writer (an existing structural test enforces this). The window's speculation task captures and scans, but never writes to the store. After the check completes, the install runs on the same task with no await in between.

C3 stays **off** until this guarantee is shown in the running configuration. The witness is the store gate itself: when a server acquires the gate, the gate records the backend it was acquired through (`store_gate::server_holds`), and releasing the gate removes it. The DDL handler takes a snapshot only when its registry backend is that exact backend. The runner gives its sources the backend it acquired the gate through. Sources built without it (library use, harnesses, tests that do not take the gate) never install, and always invalidate. Any future mode where several workers share a store (fleet audit 05, mode 1b) must provide an equivalent fence or keep C3 off.

A table whose timeline was cached (an ALTER on a busy table) then needs one read-only check instead of a reload. An uncached table (a new CREATE TABLE) still reloads once, on its first row.

**Result per single-table DDL:** 2 write transactions (before #178: 3; now: 8). Read-only work: the existing replay check, the plan's batched read, and either the C3 check (cached table) or one reload (uncached table). The statement-count lane fixes these as ceilings (§5).

**Not proposed here:** merging proofs across DDLs (DDL-storm throughput; proofs stay one DDL at a time), binlog seek cost at large offsets, or any change to locking rules. Those remain follow-ups.

## 4. Safety invariants (each gets a failing test first)

1. **Locking.** Every guarded-namespace write is inside a batch that takes the store lock as its first statement (#178). C1 adds no unlocked guarded write and removes none of the existing lock rules.
2. **Commit identity.** Every C1 batch carries a commit marker. An ambiguous commit is resolved only from the marker, never from later key values (#179). `BatchIdentityConflict` behaves as today.
3. **Atomicity, per table.** For each proven table, the registration and the binding are in the same batch and commit together or not at all. After C1, a crash can no longer leave a registered but unbound version from a proof. Atomicity across the tables of one multi-table DDL holds only when they share one batch (§3, batch boundaries). Across batches, each committed batch stands, and invariant 11 describes the state that is left.
4. **Numbering.** Version numbers are identical to the serial path's: never reused across lineages, failback or legacy history. A concurrent registration of the same key fails the plan's preconditions (`Conflict`, nothing written). The proof then re-plans from fresh reads, up to `BATCH_CONFLICT_RETRIES` times. After that the source stops with an error: the pending record stays, nothing of that batch is written, and the checkpoint does not advance.
5. **Validation.** The binding names a version that exists once the batch commits. C1 checks this inside the plan (a version created by the same batch, or an existing one with the same hash) instead of with a separate pre-read. A binding naming a missing version still fails the table, as today.
6. **Replay (review correction 4).** Before any capture, each pending DDL's table is read, and the existing binding is validated by `resolve_binding`, as `prove` does today. It checks exactly one binding, a `ddl` record at the same position, a forward proof from a supported classifier, the registry's hash of the bound version, an end at or after D, and a capture identity that re-derives from the content. Only `Ok(Some(_))` skips the capture. A binding that fails any check stops the source (`TimelineError`, reported as a schema error) and is never proven again over later server state. Binding presence or matching bytes alone never skips a capture. C1's `LogAppendIdempotent` rule is a write rule (an identical repeat answers with the stored sequence; different bytes fail closed). It is not a substitute for this validation. Replay never installs into the cache (C3 requires `Inserted`).
7. **Cancellation (review correction 1).** The pending record (C2) is already durable when the capture starts.
   - A stop before a C1 batch commits leaves the DDL pending: no new registration or binding from that batch, no checkpoint advance and no DDL event. The restart replays the DDL, finds it pending and proves it again.
   - A stop after a batch commits leaves that batch's bindings durable with the DDL event not yet emitted. On restart, replay (invariant 6) validates them and skips their capture.
   - Captures stay cancellable exactly as in #180 and #182.
8. **Saturation (review correction 1).** Unchanged from 5d: the capture comes before C1. A saturated capture raises `mysql_control_plane_saturated` and stops the source. The pending record stays; no registration or binding is written for that DDL; the checkpoint stays before the DDL. The restart replays the DDL and proves it again.
9. **Limits.** Every batch stays within the window's cost budget and the store limits (§3, batch boundaries). A table is never split across batches. An oversized single table stops the proof with a classified incident before any of the proof's batches commit.
10. **Cache coherence.**
    - A cached timeline never contains a record that is not durable: install happens only after the commit, with committed sequences.
    - An install happens only when all of these hold: the source holds the store-gate witness; the table stream after the snapshot head is exactly the pending record and the binding; neither barrier stream has advanced; the cache still holds the snapshot `Arc`; and admission accepted the augmented timeline.
    - Freshness at the check is extended through the install only by writer ownership (§3 C3): the store gate, unique source ids, and one writing task per source.
    - In every other case the timeline is invalidated, and the next row reloads it. That reload is the fallback, and it yields the same timeline an install would have.
11. **Partial multi-table DDL.**
    - Tables that fail the coverage or relevant-DDL checks are excluded before planning and stay pending, as today.
    - The remaining tables are committed in one batch when they fit, otherwise in several (invariant 3).
    - After any crash or stop, each table is either bound (validated on replay by invariant 6) or pending (proven again). Never registered without a binding, and never half a table.

## 5. Tests

All are written failing first, against the current code where the behavior differs.

- **Write-transaction count.** The test backend counts every write call that reaches the store (each is one transaction on PostgreSQL). A single-table DDL makes exactly 2: the pending record and one batch. A multi-table DDL makes one batch per group. On PostgreSQL itself, the harness measures statements and store-lock calls per DDL with `pg_stat_statements` (section 7).
- **Crash injection** at every write position (`AfterWrites(k)`) in C1, for one batch and for a DDL split across batches. The replay result equals an uninterrupted run: no lost or duplicated DDL event, no registered but unbound version, no half-written table.
- **Ambiguous commit:** resolved from the marker only, with C3's sequences taken from the marker outcome.
- **Concurrent registration** of the same key during a proof: conflict, re-plan, the same numbering as the serial path, then a stop with the record still pending once the retries are exhausted.
- **Replay:**
  - an identical binding replays without a capture;
  - a forged binding with different bytes fails closed;
  - each `resolve_binding` evidence failure (wrong position, unsupported classifier, wrong hash, end before D, a capture identity that does not re-derive, two bindings) stops the source, even when the binding's bytes match what the proof would write.
- **Batch boundaries:**
  - a multi-table DDL just under and just over the budget (one batch versus two);
  - a crash between the two;
  - an oversized single table and a single item over 16 MiB, each stopping with the classified incident and nothing written.
- **Cache races (C3), each against the fallback reload:**
  - another append to the table stream between the snapshot and the pending record;
  - another append between the pending record and the commit;
  - a lineage barrier and a database barrier appended by another writer before the check;
  - an in-process `barrier_appended`, an eviction and `invalidate_all` between snapshot and install;
  - a replayed pending record (`AlreadyPresent`);
  - an ambiguous commit;
  - an admission refusal;
  - an install that would exceed the table or byte bound.

  - an append or a barrier after the check but before the install (a test seam in that interval);
  - the same seam without the store-gate witness, where C3 never installs.

  For every case except the seam: the cached timeline after the DDL equals a fresh read, or the table is not cached.

  The seam case cannot pass by detection. It shows that the guarantee comes from ownership:
  - a second writer process cannot start while the gate is held (its gate acquisition fails before any write);
  - a second pipeline with the same source id is refused;
  - a structural test extends the barrier-writer check to every activation-stream writer, and fails on a writer outside the source's run task;
  - without the witness, the seam's write is followed by a reload, so the next row sees it. The caller's change is covered: an installed key survives `prove`'s return, and a key that was not installed is invalidated.
- **Cancellation** before and after each batch commit, and **saturation**: the record stays pending, with no registration or binding from an uncommitted batch, and the checkpoint behaves as in invariants 7 and 8.
- **Mutants:** 5 to 10, in an isolated worktree.

## 6. Explaining the remaining gap (loaded +13%, burst about 2x)

Before the fix is called complete, every run records per-phase timing histograms: the distribution of each phase's duration over the run's DDLs. They are distributions, not individual DDL traces, and are diagnostic only (they are not acceptance metrics). The phases are: pending record, replay check, capture, scan, plan, commit, C3 check or reload, emit, and time waiting for a store-pool connection. This runs in the same three comparisons. The hypotheses to confirm or reject:
- **Store round trips under contention.** Each extra locked transaction is about 4 round trips, competing with the coordinator's checkpoint commits for the shared store pool. C1 and C3 remove most of them.
- **Cache churn in the 4,200-DDL phase.** 4,200 new tables plus 150 busy ones exceed the 4,096-table cache, so busy timelines are evicted and reloaded (13,610 evictions were counted in the following window). This would be a 5e effect independent of C1.
- **Store growth:** index and stream size at the full-run scale.

**Instrumented builds (review correction 5).** The surviving binaries cannot record these phases, so every compared revision is rebuilt with the same instrumentation:
- **The instrumentation** is a `perf-phases` cargo feature. It records phase durations into in-memory histograms, written once at the end of the run, with no per-event logging. A histogram gives a phase's distribution (count, sum, percentiles); it does not attribute a slow DDL's time to its phases. It lands with the change. For the old revisions, it is backported as one patch file per revision, kept under the harness directory (`patches/f0171b4e-perf-phases.patch`, `patches/3ff3773c-perf-phases.patch`) and applied to a clean worktree of that exact commit.
- **Build identity is recorded for each binary:**
  - source commit;
  - the SHA-256 of the patch;
  - `rustc -vV`;
  - `Cargo.lock` hash;
  - profile and features;
  - the binary's SHA-256.

  The build script is committed with the harness, so any build can be reproduced.
- **Overhead check, before any comparison:**
  - for each revision, run the instrumented and uninstrumented builds interleaved (at least 3 repetitions each) on the unloaded 300-DDL test and on the loaded test;
  - the instrumentation is accepted if, for every end-to-end metric in §7, the median difference is within that metric's §7 allowance and below 3% (or within its absolute tolerance for near-zero metrics);
  - otherwise the instrumentation is reduced, and the comparisons use uninstrumented builds for cost and instrumented builds for the phase breakdown only.

## 7. Validation after implementation

**Builds:** old (f0171b4e), current main (3ff3773c) and the change, each instrumented as in §6, on fresh state for every repetition.

**Comparisons:**
- 300 DDLs unloaded;
- 1,000 DDLs under load;
- the full run's 4,200-DDL phase (the burst);
- SQLite fresh activation, burst activation and restart (the ruling's re-measurement).

**Repetitions:** at least 3 per build for every comparison, burst included. The order rotates: ABC, BCA, CAB.

**Metrics, for every repetition:**
- phase completion time;
- per-DDL cost (mean and p50/p99), with the §6 phase breakdown;
- busy-table lag (p50, p99, max);
- drain time.

**Variation rule (fixed before any results are seen).** The rule applies only to the comparable end-to-end metrics: phase completion time, mean per-DDL cost, busy-table lag (p99 and max) and drain time. Phase histograms and other diagnostics are reported, not judged.
- For each metric and build, the same-build spread is (max - min) / median over that build's repetitions.
- **Allowance:** the larger of (a) the larger of the two builds' spreads, times the old binary's median, and (b) the metric's absolute tolerance. Absolute tolerances: per-DDL cost 2 ms; busy-table lag 50 ms; drain time 1 s; phase completion time 1 s. These keep near-zero metrics, such as unloaded lag, from being judged on noise.
- The change is "no worse" than the old binary on a metric when both of these hold:
  - its median is at most the old median plus the allowance;
  - its worst repetition is at most the old binary's worst repetition plus the absolute tolerance.
- **Inconclusive results:**
  - A metric whose spread exceeds 15% for either build, and is above its absolute tolerance, is inconclusive. That comparison is rerun with 5 repetitions per build, and both runs are reported.
  - If the rerun still exceeds 15%, the result stays inconclusive. It is reported as such, acceptance is not claimed for that metric, and it goes back to the reviewer.

**Drain and correctness, for every pipeline and repetition, from its start position:**
- **When drain is complete:**
  - every sink of the pipeline has a durable delivery frontier (its committed checkpoint in the checkpoint store) at or after the generator's final binlog position (GTID set); and
  - the delivered rows equal the generator's ledger from the pipeline's start position: the same rows, each exactly once, in per-key order.

  Neither the source position nor a stable sink count is used. Either can look complete while a sink is stalled.
- **Bounded drain:** the drain must complete within a timeout fixed per comparison before the runs (10 minutes for each DDL comparison; 5 minutes for the SQLite runs). A timeout fails the repetition, which is retained and reported. It is never retried silently.
- **Reported:** missing, duplicate and per-key reordered rows, all checked against the ledger after the drain completes.

**Acceptance:**
- PostgreSQL phase completion time, per-DDL cost, busy-table lag and drain time are no worse than the old binary in all three DDL comparisons.
- SQLite shows no regression in any of its comparisons.
- Zero missing, duplicate or reordered rows in every repetition.

## 8. Harness and retention

**Location:** `crates/perf-harness/actdiag/`. It holds:
- the driver and analyzers;
- the build script and the instrumentation patches (§6);
- workload definitions and configurations.

It is excluded from the core gate. Its own smoke check (`crates/perf-harness/actdiag/smoke.sh`) runs a 10-DDL unloaded comparison of one build against itself in a few minutes, and checks the drain and correctness logic end to end. The smoke check runs before every measured series and on any change to the harness.

**Retained for every run, under `target/perf/evidence/<run>/`:**
- the exact commands;
- configurations and seeds;
- the build identities (§6);
- environment details: kernel, CPU model and governor, memory, disk, Docker version, image digests, MySQL and PostgreSQL versions and settings;
- raw per-repetition data;
- a SHA-256 manifest.

**Durable copy:**
- The manifest and the write-up are committed. A committed checksum detects changes; it does not preserve the data.
- After each completed series, its run directories are copied to `$HOME/deltaforge-evidence/<run>/`, outside the repository and the build directory, and the copy is verified against the manifest. That happens before any run resources (containers, volumes, `target/` data) are removed.
- A second copy off this machine is recommended; its location is the maintainer's choice.
