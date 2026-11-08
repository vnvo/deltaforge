# Release Checklist & Acceptance Criteria

The finite, evidence-backed gate for cutting a DeltaForge release. Every criterion has exactly one status:

- **PASS** - met, with test, report or commit evidence.
- **ACCEPTED LIMITATION** - a known boundary that does not block release, with the operator control stated and documented in the [Supported Deployment Envelope](../src/deployment-support.md).
- **BLOCKED** - not met; prevents release.

**Release decision rule:** release only when no criterion is BLOCKED and every ACCEPTED LIMITATION is in the Supported Deployment Envelope with its operator control. Work listed under [Deferred, not in this release's scope](#deferred-not-in-this-releases-scope) is neither a criterion nor a support claim.

**Evidence baseline:** `main` at `68c3fc17` (PR #206). Its tree is identical to `59438b86`, whose full core gate passed (65 suites, 4,270 tests; evidence `core-c10-59438b86`, manifest `380fea43`). The release commit (v0.1.0-rc.1) changes only release documents and the chart's `appVersion`, and is checked by G1 and G2.

## Release gates

These are the remaining steps before release. Each is a single, exact-commit check.

| # | Gate | Status | Evidence required |
|---|------|--------|-------------------|
| G1 | Release-tier gate `scripts/gate.sh --release` green on the exact release commit (the core tier plus the sink suites and the MiniStack S3 canaries) | **PENDING** - run on the release commit | Gate output with every suite passing, the tree unchanged from start to end, retained with a manifest |
| G2 | CI green (lint, test, coverage) on the exact release commit | **PENDING** - run on the release commit | CI run on that commit |
| G3 | MySQL T1a qualification ([scope](#g3-mysql-t1a)) | **PASS** - S1, A1, A2, A3 and S3 complete (2026-11-08): no missing, duplicate or out-of-order event, no pipeline failure, no incident. S1 on `42d3f77e`; A1 to A3 on `42d3f77e` with harness `543d20ca` (fresh-start setup); S3 on `3f63a814` after the activation-window fix (#205), with a fresh source and store. Carried to the release commit by ruling (2026-11-08): the only later code change is the retained-state binding (#206) | `~/deltaforge-evidence/qualification-42d3f77e` (`g3/`, `corrected-543d20ca/`, `s3-3f63a814/`; the failed S3 attempt and the earlier A1 attempts are kept), manifest `b69aa5e8` |
| G4 | PostgreSQL source qualification ([scope](#g4-postgresql-source)) | **PASS** - on `3f63a814` (2026-11-08): 20,000 snapshot rows, 379,992 changes committed and delivered, no missing, duplicate or out-of-order change, no reconciliation mismatch; stop/resume and `ADD COLUMN` clean; TOAST reconstructed 4,454 and unavailable 4,504, each counter equal to its delivered count; both unsupported cases failed visibly as specified; no incident in the main pipeline. Carried to the release commit by the same ruling as G3 | `~/deltaforge-evidence/qualification-42d3f77e/g4-3f63a814`, manifest `b69aa5e8` |
| G5 | Release documentation matches the code at the release commit (this checklist, the Supported Deployment Envelope, the CHANGELOG) | **PASS** at the release commit | This revision |

G1 and G2 pass only on the release commit. G3 and G4 run on the candidate commit; their evidence carries to the release commit only if the code is unchanged between the two. If qualification needs a code change, the reruns are decided from that change's scope. A failure is kept and investigated, never rerun until it passes. Every qualification run starts with at least 50 GiB free and stops if free disk falls below 25 GiB.

**Neither G3 nor G4 qualifies the PostgreSQL state store.** Both use the SQLite state store, the supported production backend; PostgreSQL state-store qualification is deferred.

### G3: MySQL T1a

The exploratory tier T1a of the [MySQL fleet qualification design](../design/mysql-fleet-qualification.md): one MySQL 8.4 server with 200 databases of 200 tables (40,000 tables), one pipeline, the release image, scenarios S1 (30 minutes after a 2-minute warm-up), A1 to A3 (active sets of 0.01%, 0.1% and 1%), and S3 (a migration DDL storm, last because it changes the fixture). One run each.

**Departure from the earlier plan:** T1a was planned with the PostgreSQL state store. It runs on SQLite, the supported production backend, so that the qualification covers what the release supports.

### G4: PostgreSQL source

- **Setup:** PostgreSQL 17, one source and one pipeline, a required Kafka sink (plaintext), the SQLite state store, the release image, a registered publication.
- **Workload:** an initial snapshot of 1,000 tables, then 30 minutes of steady CDC across them with inserts, updates and deletes; one stop and resume during the CDC; one `ADD COLUMN` on a table that is receiving rows.
- **TOAST, supported cases (must deliver correctly):** tables with large out-of-line values under `REPLICA IDENTITY FULL`, with updates that change those values and updates that leave them unchanged: every event's `after` equals the committed row, unchanged values filled exactly. The counter delta over the run (`deltaforge_pg_unchanged_toast_total{outcome="reconstructed"}`, and `unavailable` below) is at least the delivered count of such values, with both reported: an emission before the stop can be emitted again after the resume, so equality is not required. Row-content reconciliation stays exact. A counter that cannot be read (a failed scrape, an invalid value, a decrease) leaves G4 incomplete, never zero. Tables under `REPLICA IDENTITY DEFAULT`, with the native JSON sink: an unchanged out-of-line value is left out of `after` and listed in `unchanged_columns` (counted as `unavailable`).
- **TOAST, deliberately unsupported cases (must fail visibly, counted separately):** run in a separate pipeline whose sink needs complete rows. A captured table with a TOAST-capable column without `REPLICA IDENTITY FULL` must be refused at start (`pg_table_unsupported`, class `toast_without_full_identity`) with nothing snapshotted and no checkpoint; changing a captured table's identity away from `FULL` while running must stop the source before that table's next row, with failed status and an incident. These expected refusals are reported on their own and are not G4 failures; any other outcome (a partial row delivered, streaming that continues) fails G4.
- **Reconciliation (exact):** after the final drain, for every primary key the last delivered row equals the source table's final row in every column (by canonical value), across the snapshot-to-CDC handover, the stop and resume, and the `ADD COLUMN` (rows after it carry the new column). Every committed change is delivered at least once, in per-key commit order; duplicates are allowed only in the snapshot overlap and the restart replay, and are counted.
- **Health:** the main pipeline's status is polled throughout and incidents are counted; any pipeline failure or incident in the main pipeline fails G4.

## Correctness

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| C1 | Required-sink and per-row acknowledgements fail closed (no silent data loss) | **PASS** | PR #103; `crates/runner/src/coordinator.rs` and `dlq.rs` tests (`queue_push_backend_failure_returns_dropped`, required and optional sink hold-checkpoint tests) |
| C2 | Failover identity fails closed (a verified nonzero lineage before streaming; no fail-open compare or persist) | **PASS** | PR #104; `failover_e2e` and `identity_fail_closed_tests` |
| C3 | Lifecycle safety: start, stop, delete and patch are serialized; delete fails closed; a duplicate source id is rejected; a second server against the same state store refuses to start | **PASS** | PR #105; `pipeline_manager` unit tests; storage `slot_create` one-winner; the [store gate](../src/deployment-support.md#store-gate) |
| C4 | Source authority fails closed: PostgreSQL schema reload propagates; MySQL snapshot progress and non-GTID lineage fail closed | **PASS** | PR #106; `load_snapshot_progress` and `mysql_server_lineage` unit tests; PostgreSQL reload live tests |
| C5 | Live fault and lifecycle suites green on the evidence baseline | **PASS** | The core gate on `dba04df2` (64 suites), including `failover_e2e`, the snapshot restart suites, `txn_coordinator_e2e` and `kafka_outage_restart_e2e` |
| C6 | Internal reliability soak recorded | **PASS** | PR #110; [reliability-soak.md](reliability-soak.md) |
| C7 | MySQL rows are emitted only when decoded with the schema version proven for their binlog position and consistent with the layout the binlog recorded; otherwise never emitted | **PASS** | `mysql_proof_matrix_e2e`, `mysql_ddl_replay_failclosed`, and the `mysql_selection`, `mysql_signature` and `mysql_activation` unit tests |
| C8 | MySQL replay of binlog rows across a DDL | **ACCEPTED LIMITATION** | Supported when the version in effect is provable (C7); otherwise the source stops fail-closed. **Operator control:** set `binlog_row_metadata=FULL`, keep the pipeline caught up before applying DDL, and re-snapshot on this failure. See [Guarantees](../src/guarantees.md#mysql-replay-across-a-schema-change) |
| C9 | A MySQL activation window never admits a table its capture does not contain: a capture that started before a table's rows defers the table and recaptures after a bounded readiness wait; anything not provably ordered, or still behind after the wait, fails closed with nothing dispatched and the checkpoint unchanged | **PASS** | PR #198: `a_capture_behind_a_tables_rows_defers_that_table` (red before the fix), `a_wait_that_times_out_or_is_cancelled_dispatches_nothing`, `a_capture_still_before_the_rows_after_a_covered_wait_fails_closed`, `an_unordered_capture_start_still_fails_closed` and the `mysql_behind_wait` tests |
| C10 | Recreating a pipeline (same name and source id) adopts retained state only when the new spec is compatible with the one that wrote it: tenant, source selection and sink identities are checked before adoption, and a mismatch refuses | **PASS** | PR #206 (`68c3fc17`), core gate on `59438b86`: the binding (`docs/design/retained-state-compatibility.md`) recorded at the first start and compared on every create and PATCH before anything is claimed, stopped or written; protected state includes PostgreSQL slots and S3 durable HEADs; undeterminable state fails closed; the Elasticsearch and ClickHouse `destination_changed` fence; single-source adoption with a proof over state contents. `state_binding` unit and crash-point tests, `a_refused_create_leaves_the_original_binding_unchanged`, `publication_contract_e2e` (adoption against a live server) |

## Delivery semantics

| # | Criterion | Status | Evidence / operator control |
|---|-----------|--------|------------------------------|
| D1 | At-least-once delivery; consumers dedup on the event `id` | **ACCEPTED LIMITATION** | No sink is end-to-end exactly-once. **Operator control:** consumers dedup on the event `id`. See [Guarantees](../src/guarantees.md) |
| D2 | A pipeline that stops fail-closed stays stopped until an operator restarts it | **ACCEPTED LIMITATION** | There is no internal auto-restart. Terminal required-sink and checkpoint-store failures, and every source error that fails closed (including the MySQL activation window's), stop the pipeline. `/ready` returns 503 while any pipeline is failed or starting, has a blocking incident or has a recovery pending; `/health` stays 200, so Kubernetes does not restart the pod. **Operator control:** alert on `/ready` or `deltaforge_pipeline_status`, and restart the pipeline (stop then resume) or the process once the cause is understood. See [reliability-soak.md](reliability-soak.md) |

## Deployment & operations

| # | Criterion | Status | Evidence / operator control |
|---|-----------|--------|------------------------------|
| O1 | One DeltaForge server per state store | **PASS** | Enforced within a process (serialized lifecycle, durable source-id claim) and across processes by the store gate: a second server against the same store refuses to start. A crash leaves the gate held until an operator releases it. See [Store gate](../src/deployment-support.md#store-gate) |
| O2 | No transport encryption to source databases, a PostgreSQL state store, or Kafka and Redis sinks; Kafka SASL `PLAIN` only (credentials in clear); some TLS settings accepted but ignored; the REST API and metrics are unauthenticated | **ACCEPTED LIMITATION** for this beta, **trusted, isolated networks only** | Not suitable where transport encryption is required. **Operator control:** run on a trusted, isolated network; bind the API and metrics to loopback or confine them with a firewall or NetworkPolicy (metrics are also served on the API port); do not rely on the ignored settings listed in [Network and TLS](../src/deployment-support.md#network-and-tls) |
| O3 | Deployment preflight validates the source, configuration and credential references before start | **PASS** | PR #107; `preflight_e2e`; the Helm preflight initContainer |
| O4 | Preflight does not cover every startup requirement | **ACCEPTED LIMITATION** | Preflight does not probe sink endpoints, and does not check the MySQL version, `log_replica_updates` or PostgreSQL publication registration (startup checks all three and refuses to run). It always requires MySQL `RELOAD` and `gtid_mode = ON`, also for a CDC-only pipeline. **Operator control:** validate sink connectivity during onboarding, and treat a startup refusal as the authoritative check. See [Deployment preflight](../src/deployment-support.md#deployment-preflight) |
| O5 | Production quick start in the supported shape | **PASS** | PR #109; [Production Quick Start](../src/quickstart-production.md) |
| O6 | SQLite state backup and restore procedure documented | **ACCEPTED LIMITATION** | **Operator control:** back up the state store with the WAL-safe procedure; a lost store loses the checkpoint and needs a restore or a re-snapshot. See [State and backup](../src/deployment-support.md#state-and-backup-requirements) |

## Capacity & scale

| # | Criterion | Status | Evidence / operator control |
|---|-----------|--------|------------------------------|
| S1 | Capacity guidance is a starting point, not a tested ceiling | **ACCEPTED LIMITATION** | Every bound in the [Capacity & Resource Envelope](../src/capacity-envelope.md) is classified (measured, code-derived, external, operator, unknown); there is no aggregate memory cap. **Operator control:** soak in the target environment before relying on a number |
| S2 | Large catalogs, dense active sets and fleets are unqualified | **ACCEPTED LIMITATION** | Single instance only. Qualified scope ends where G3 and G4 end; everything larger is listed under [Unqualified deployments](../src/deployment-support.md#unqualified-deployments). **Operator control:** stay inside the qualified scope or validate in a disposable environment first |

## Release engineering

| # | Criterion | Status | Evidence / control |
|---|-----------|--------|--------------------|
| R1 | Rollback procedure defined | **PASS** | [Rollback](#rollback-procedure) below |
| R2 | Minimum observability checks defined | **PASS** | [Observability](#minimum-observability-checks) below |
| R3 | Release-tier gate and CI on the release commit | see G1, G2 | - |

## Open incidents

| # | Incident | Status | Rule |
|---|----------|--------|------|
| I1 | One unexplained SIGBUS of the `sources` library test binary in a CI coverage run at `df726153`; it passed on rerun and on every later revision | **ACCEPTED, unresolved, non-blocking** | Any recurrence blocks release and requires diagnosis. Coverage is not rerun only to pursue the historical failure |

## Performance claims

No performance figure is a release criterion, and neither this checklist nor the Supported Deployment Envelope cites one. Figures elsewhere in the documentation are development-machine observations (see [Capacity & Resource Envelope](../src/capacity-envelope.md#throughput)). The release notes cite only figures whose retained runs show every row delivered and no runner error; where the raw output was not retained, the figure is marked as such.

## Deferred, not in this release's scope

Not criteria for this release, and not support claims:

- Capacity sweeps beyond the qualified scope: more tables per source (100,000 and above), dense active sets (20 to 100% of tables), rotating active sets, multi-source fleets, and the load on the source database at those scales.
- Further tuning of activation latency, schema capture, the selection cache and memory.
- Real-AWS S3 qualification.
- PostgreSQL state-store qualification (the backend stays beta).
- TLS for source databases, the PostgreSQL state store, Kafka (with its other SASL mechanisms) and Redis, and applying the ignored ClickHouse and Elasticsearch TLS settings.

## Rollback procedure

Single-instance StatefulSet, so rollback is a controlled replace, not a scale event:

1. **Pin forward and back by commit or tag and image digest.** Keep the previous release's chart values and image digest.
2. **Roll back the deployment**: `helm rollback <release> <previous-revision>` (or redeploy the previous chart and image). The StatefulSet terminates the old pod before starting the new one, and the preflight initContainer gates the rollback again.
3. **State-store compatibility**: the checkpoint and schema store is shared across versions. Roll back only to a version with a compatible on-disk format; if a release changed the checkpoint or schema-registry format, treat the state store as forward-only and follow that release's notes (a rollback may need a restore from backup or a re-snapshot).
4. **Verify** `/ready` returns 200 and the expected pipelines are `running` before restoring traffic.
5. **If the state store is suspect**, restore from the last good backup (accept at-least-once re-delivery after the backup point) or re-snapshot.

## Minimum observability checks

Confirm before and after release:

- **Liveness**: `GET /health` returns 200 while the process runs, also when a pipeline is failed.
- **Readiness**: `GET /ready` returns 200 when every pipeline is healthy, and 503 naming the offender while any pipeline is failed or starting, has a blocking incident or has a recovery pending.
- **Metrics**: Prometheus metrics are served on the metrics listener (default `0.0.0.0:9000`) and also on the API listener (default `0.0.0.0:8080`). They include `deltaforge_pipeline_status` per pipeline, source and sink counters and stage-latency histograms. Restrict exposure per O2.
- **Pipeline status**: `deltaforge_pipeline_status` is 1 running, 0.5 paused, 0.25 starting, 0 stopped and -1 failed; alert on -1 (an operator restart is needed, D2).
- **Incidents**: `deltaforge_incidents_raised_total` counts every incident raised; investigate any increase.
- **Logs**: `RUST_LOG` set to an operable level; a failed pipeline logs a typed, actionable error.

Evidence: `crates/rest-api/src/health.rs`, `crates/o11y/src/df_metrics.rs`, `crates/runner/src/pipeline_manager.rs`, [Observability](../src/observability.md).

## Acceptance

A release is accepted when:

1. G1 to G5 are **PASS** on the release commit.
2. C1 to C7, C9, C10, O1, O3, O5 and R1 to R2 are **PASS**.
3. Every ACCEPTED LIMITATION (C8, D1, D2, O2, O4, O6, S1, S2) is in the Supported Deployment Envelope with its operator control.
4. No criterion is **BLOCKED**, and open incident I1 has not recurred.

Status at the release commit (v0.1.0-rc.1): G3 to G5 and C10 **PASS**; G1 and G2 pending on this commit.
