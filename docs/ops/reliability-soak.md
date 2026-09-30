# Internal Reliability Soak - Results

Fault/lifecycle soak of the real source → coordinator → sink pipeline, recorded per scenario for release sign-off. Results feed the release checklist and acceptance criteria directly.

## Run metadata

- **Build**: merged `main` at commit `feef75d` (crate version 0.1.0).
- **Harness**: `crates/runner/tests/reliability_soak_e2e.rs` (PostgreSQL 17 + Confluent cp-kafka 7.5 via testcontainers), run `--include-ignored --nocapture --test-threads=1`. **4 passed / 0 failed** in 303s.
- **Schema scenarios**: run from the existing live suites `postgres_cdc_e2e` / `mysql_cdc_e2e` / `failover_e2e` (green in the consolidated fault/lifecycle matrix on the same `main`).
- **Config per scenario**: PG logical replication, `snapshot.mode = never`, one **required** Kafka sink, `respect_source_tx = true`, `max_inflight = 1`, `send_timeout = 4s`.

## Recovery model (important)

DeltaForge currently has **no internal automatic pipeline restart**. When a required sink or the checkpoint store fails, the pipeline **terminates fail-closed** and stays down; restoring the sink or the storage does **not** restart it. Recovery requires an **explicit pipeline/process restart** (the harness performs this with a second run). Note that standard Kubernetes probes do **not** recover it: `/health` stays 200 (so the pod is not restarted) and `/ready` returns 503 (which only removes the pod from Service endpoints). In production an operator - or external automation that watches `/ready` or `deltaforge_pipeline_status` and deliberately acts - must restart the pipeline (stop/resume via the API) or the process/pod (e.g. `kubectl rollout restart`). Until that restart succeeds, `/ready` stays 503. Automatic pipeline supervision is future work and is recorded as such in the release checklist.

## HTTP readiness / health (recorded separately)

This harness drives the pipeline components directly and does **not** run the REST layer, so its per-scenario field is **"pipeline task state"**, not HTTP health. The HTTP behaviour is covered by the rest-api readiness regression (`crates/rest-api/src/lib.rs`):

- `/health` (liveness) **stays 200** while the process/API event loop is alive, even when a pipeline is `failed` - so Kubernetes does not restart the whole process for an intentionally fail-closed pipeline (e.g. schema drift under Halt), which would loop forever.
- `/ready` (readiness) returns **503** and names the failed pipeline when any pipeline's status is `failed`, so Kubernetes drains it from the Service; it returns 200 once all pipelines are healthy again.
- Tests: `failed_pipeline_makes_ready_503_but_health_stays_200`, `ready_returns_200_when_all_pipelines_healthy` (`crates/rest-api/src/health.rs:18,46`).

So for the outage scenarios below, the required-sink / checkpoint failures drive the pipeline status to `failed` → `/ready` 503 (liveness `/health` unaffected). **Readiness stays 503 until the pipeline/process is restarted** (see the recovery note below); a successful restart returns `/ready` to 200.

## Scenario ledger

### 1. Restart durability - PASS
- **Fault/duration**: stop the pipeline after run 1 (~12s); write 5 more rows while down; restart.
- **Invariant**: all rows delivered across the restart, none missing.
- **Positions**: source before `0/1512508` → after run1 `0/1512900` → after run2 `0/1512CD0`; per-sink checkpoint advanced in lockstep (`0/1512900` → `0/1512CD0`).
- **Delivery**: 10/10 distinct ids, **0 duplicates**, **0 missing**.
- **Recovery**: automatic on restart; no operator intervention.
- **Pipeline task state**: coordinator ran to idle each run; clean stop.
- **WAL retention**: slot retained rows written while down; released only after durable ack.

### 2. Required-sink outage - PASS
- **Fault/duration**: required sink unreachable (dead broker) ~20s, then restored.
- **Invariant**: checkpoint held and WAL retained while the sink is down; no loss; resume on recovery.
- **Positions**: source before `0/1512508`; **during outage `0/1512508` (unchanged)**, checkpoint `None` (held=true); after recovery checkpoint `0/1512778`.
- **Coordinator result while down**: `Err(commit policy not satisfied: required 0/1 acks, total 0)` - required-sink failure stops the pipeline (fail-closed).
- **Delivery after recovery**: 3/3 distinct ids, **0 duplicates**, **0 missing**.
- **Recovery**: **required a pipeline/process restart** after the sink became reachable (the harness started a new pipeline run; ~137s wall clock). DeltaForge does not auto-restart a terminated pipeline - **operator/supervisor intervention required**.
- **HTTP**: pipeline status `failed` during outage → `/ready` 503 (and it stays 503 until the restart); `/health` stays 200; `/ready` returns 200 after the restart.
- **WAL retention**: slot retained WAL during the outage; released after durable ack.

### 3. Checkpoint-store outage - PASS
- **Fault/duration**: checkpoint `put_raw` fails ~18s, then restored.
- **Invariant**: fail closed - no checkpoint advance and no WAL release during the outage; no loss.
- **Positions**: source before `0/1512508`; **during outage `0/1512508` (unchanged)**, checkpoint `None` (held=true); after recovery checkpoint and source `0/1512778`.
- **Coordinator result while down**: `Err(commit checkpoint … injected checkpoint-store outage)` - the checkpoint write failure surfaces fail-closed; the source position is not released after a sink ack it could not durably record.
- **Delivery**: 3/3 distinct ids, **0 duplicates observed**, **0 missing**. (Duplicates are permitted here - at-least-once re-delivery is expected if the checkpoint could not be recorded during the outage.)
- **Recovery**: **required a pipeline/process restart** after the store became writable (the harness started a new pipeline run; ~16s wall clock). DeltaForge does not auto-restart a terminated pipeline - **operator/supervisor intervention required**. `/ready` stays 503 until the restart.
- **WAL retention**: slot held WAL while the checkpoint could not advance.

### 4. Shutdown with a committed transaction in flight - PASS
- **Note**: PostgreSQL logical replication exposes a transaction's rows around COMMIT, so a timing delay cannot guarantee the source is between BEGIN and COMMIT. This scenario validates shutdown while a **committed** transaction's events are in flight. A deterministic BEGIN/COMMIT-boundary variant (pause the source after `TxBegin`, before commit, using the mid-transaction sync technique from the credential-rotation test) can be added if a stronger open-transaction invariant is required.
- **Fault/duration**: stop the pipeline ~300 ms after an 8-row transaction commits (its events in flight), then restart.
- **Invariant**: the committed transaction is delivered **whole** across the shutdown; never a partial or missing subset.
- **Positions**: source before `0/1512508`; checkpoint at stop `None`; after restart checkpoint and source `0/1512998`.
- **Delivery**: 8/8 distinct ids, **0 duplicates**, **0 missing** (the transaction delivered whole).
- **Recovery**: automatic on restart; no operator intervention.

### 5. Schema-compatible change - PASS (existing live tests)
- **Tests**: `postgres_cdc_schema_evolution` (`crates/sources/tests/postgres_cdc_e2e.rs`), `mysql_cdc_schema_reload_on_ddl` (`crates/sources/tests/mysql_cdc_e2e.rs`).
- **Invariant asserted**: an ADD COLUMN mid-stream is picked up - the post-DDL row carries the new column, the schema version bumps, the envelope stays intact, the checkpoint advances, and no restart loop occurs.

### 6. Schema-incompatible drift under Halt - PASS (existing live tests)
- **Tests**: `pg_schema_drift_halt_fails_closed_and_does_not_skip` and `pg_schema_drift_halt_instream_fails_before_post_drift_row` (`postgres_cdc_e2e.rs`), `mysql_failover_schema_drift_halts_source` (`failover_e2e.rs`).
- **Invariant asserted**: the source never becomes ready; a typed error names the table + remediation; **no** post-drift/row/TxCommit event is delivered; the checkpoint stays at the prior boundary; an unchanged restart **fails again** (never silently skips). This is the fault that drives `/ready` to 503.

### 7. Schema-incompatible drift under Adapt - PASS (existing live tests)
- **Test**: `pg_schema_drift_adapt_delivers_post_drift_and_advances_checkpoint` (`postgres_cdc_e2e.rs`).
- **Invariant asserted**: the post-drift row is decoded under the reloaded schema (new column present) **and** the checkpoint advances past the drift; a reload failure fails closed rather than emitting a row against an unverified schema.

## Summary

| # | Scenario | Result | Loss | Operator intervention |
|---|----------|--------|------|-----------------------|
| 1 | Restart durability | PASS | none | restart is the scenario |
| 2 | Required-sink outage | PASS | none (checkpoint held) | **yes - restart after sink reachable** |
| 3 | Checkpoint-store outage | PASS | none (fail-closed) | **yes - restart after store writable** |
| 4 | Shutdown, committed tx in flight | PASS | none (tx whole) | restart is the scenario |
| 5 | Schema-compatible change | PASS | none | no |
| 6 | Schema-incompatible / Halt | PASS | none (fails closed, no skip) | yes - fix schema, then restart |
| 7 | Schema-incompatible / Adapt | PASS | none | no |

Every scenario preserved the no-loss invariant; checkpoints and source WAL/binlog positions advanced only after a durable acknowledgement. **DeltaForge does not auto-restart a terminated pipeline**: the required-sink and checkpoint-store outages recover only after a pipeline/process restart (an external supervisor or operator), and `/ready` stays 503 until then. Automatic pipeline supervision is future work (tracked for the release checklist).

## Reproduce

```bash
# In-process fault scenarios (1-4):
cargo test -p runner --test reliability_soak_e2e -- --include-ignored --nocapture --test-threads=1

# Schema scenarios (5-7):
cargo test -p sources --test postgres_cdc_e2e -- --include-ignored --test-threads=1 \
  pg_schema_drift postgres_cdc_schema_evolution
cargo test -p sources --test failover_e2e -- --include-ignored --test-threads=1 \
  mysql_failover_schema_drift
```
