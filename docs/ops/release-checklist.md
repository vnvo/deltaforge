# Release Checklist & Acceptance Criteria

Binary, evidence-backed gate for cutting a DeltaForge release. Every criterion is exactly one of:

- **PASS** - met, with test/report/commit evidence.
- **ACCEPTED LIMITATION** - a known boundary that does not block release, with the required operator control stated.
- **BLOCKED** - not met; prevents release.

**Release decision rule:** release only when there are **zero BLOCKED** criteria and every ACCEPTED LIMITATION is documented in the [Supported Deployment Envelope](../src/deployment-support.md) with its operator control in place.

Evidence baseline: `main` at the release commit; PRs #103-#110.

## Correctness

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| C1 | Required-sink / per-row acks fail closed (no silent data loss) | **PASS** | PR #103; `crates/runner/src/coordinator.rs` + `dlq.rs` tests (`queue_push_backend_failure_returns_dropped`, required/optional-sink hold-checkpoint tests) |
| C2 | Failover identity is fail-closed (verified nonzero lineage before streaming; no fail-open compare/persist) | **PASS** | PR #104; `failover_e2e` 10/10 (final matrix) + `identity_fail_closed_tests`; zero-open counted seam |
| C3 | Lifecycle safety: serialized start/stop/delete/patch; delete fail-closed; duplicate source-id rejected | **PASS** | PR #105; `pipeline_manager` unit tests (concurrency, injected-failure, `Deleting` state); storage `slot_create` one-winner |
| C4 | Source authority fail-closed: PG schema reload propagates; MySQL snapshot-progress + non-GTID lineage fail closed | **PASS** | PR #106; `load_snapshot_progress` / `mysql_server_lineage` unit tests; PG reload live tests |
| C5 | Green live fault/lifecycle matrix on merged main | **PASS** | Consolidated matrix: `failover_e2e` 10, PG/MySQL snapshot, `txn_coordinator_e2e`, PG schema-drift subset, MySQL schema-reload, throughput - 38 tests, 0 fail |
| C6 | Internal reliability soak recorded | **PASS** | PR #110; [reliability-soak.md](reliability-soak.md) - restart, required-sink outage, checkpoint-store outage, shutdown with committed tx in flight (4/4) + schema compatible/Halt/Adapt |

## Delivery semantics

| # | Criterion | Status | Evidence / operator control |
|---|-----------|--------|------------------------------|
| D1 | At-least-once delivery; consumers dedup on event `id` | **ACCEPTED LIMITATION** | No sink is end-to-end exactly-once. **Operator control:** consumers must dedup on the event `id`; snapshot→CDC has a bounded at-least-once overlap and restarts re-deliver. Evidence: [Guarantees](../src/guarantees.md), soak report (0 duplicates observed but the contract is at-least-once). |
| D2 | Terminal required-sink and checkpoint-store failures require an explicit restart | **ACCEPTED LIMITATION** | DeltaForge has **no internal auto-restart**: on a required-sink or checkpoint-store terminal failure the pipeline stops fail-closed and `/ready` returns 503, but **`/health` stays 200, so Kubernetes does NOT restart the pod** - readiness only removes it from Service endpoints. **Operator control:** perform an explicit recovery action once the fault clears - a pipeline restart (stop then resume via the API) or a pod/process restart (e.g. `kubectl rollout restart`). Any external automation must **watch `/ready` or `deltaforge_pipeline_status` and deliberately invoke that action**; standard Kubernetes liveness/readiness behaviour alone does not recover the pipeline. Readiness returns to 200 after a successful restart. Automatic pipeline supervision is future work. Evidence: [reliability-soak.md](reliability-soak.md) (Recovery model). |

## Deployment & operations

| # | Criterion | Status | Evidence / operator control |
|---|-----------|--------|------------------------------|
| O1 | Single DeltaForge process per state store | **ACCEPTED LIMITATION** | Enforced within one process (serialized lifecycle + durable source-id claim), **not** across processes. **`ReadWriteOnce` is not a cross-process lock** (it restricts volume attachment to one node; pods on that node could still mount it). **Operator control:** the control is `replicaCount: 1` plus the prohibition against starting a second installation against the same state store - do not scale the replica count or point a second install at the same store. Evidence: PR #105; [deployment-support: single owner](../src/deployment-support.md#topology-single-owner-per-source). |
| O2 | Source DB connections have no TLS; REST API and metrics are unauthenticated | **ACCEPTED LIMITATION** | **Operator control:** run on a trusted/private network or via an encrypted tunnel; bind the API/metrics to loopback or confine with a firewall/NetworkPolicy. Evidence: [deployment-support: Network and TLS](../src/deployment-support.md#network-and-tls); [Observability](../src/observability.md#metrics-endpoint-address-and-exposure). |
| O3 | Deployment preflight validates source + config + credentials before start | **PASS** | PR #107; `preflight_e2e` (fresh/owned/foreign slot, missing sink secret, unavailable ownership store); Helm preflight initContainer gate |
| O4 | Sink endpoint reachability is not covered by preflight | **ACCEPTED LIMITATION** | Preflight validates the source, config, commit policy, and source+sink credential *references*, but does not connect to sink endpoints. **Operator control:** validate sink connectivity separately during onboarding. Evidence: PR #107; [deployment-support: preflight](../src/deployment-support.md#deployment-preflight). |
| O5 | Production quick start using the supported shape | **PASS** | PR #109; [Production Quick Start](../src/quickstart-production.md) (single-instance, persistence, referenced secrets, probes, resource limits, preflight gate) |
| O6 | SQLite state backup/restore procedure documented | **ACCEPTED LIMITATION** | **Operator control:** back up the state volume with the WAL-safe procedure (online-backup / `VACUUM INTO`, or clean stop + `wal_checkpoint(TRUNCATE)` + copy, or a consistent volume snapshot including `-wal`/`-shm`); a lost volume loses the durable checkpoint and requires restore or re-snapshot. Evidence: [deployment-support: State and backup](../src/deployment-support.md#state-and-backup-requirements). |

## Capacity & scale

| # | Criterion | Status | Evidence / operator control |
|---|-----------|--------|------------------------------|
| S1 | Capacity guidance is a starting point, not a tested ceiling | **ACCEPTED LIMITATION** | Every bound in the [Capacity & Resource Envelope](../src/capacity-envelope.md) is classified (measured / code-derived / external / operator / unknown); no aggregate memory cap exists. **Operator control:** benchmark and soak in the target environment before committing to numbers. |
| S2 | Dense-fleet / catalog-scale deployment is not yet supported | **ACCEPTED LIMITATION** | Single-instance only; per-table metric cardinality, full schema-registry load at startup, and O(tables²) enumeration are unbounded in the table dimension. **Operator control:** start with a small number of pipelines and tens-to-low-hundreds of tables per source; fleet-scale architecture is future work. Evidence: [capacity-envelope.md](../src/capacity-envelope.md). |

## Release engineering

| # | Criterion | Status | Evidence / control |
|---|-----------|--------|--------------------|
| R1 | Rollback procedure defined | **PASS** | See [Rollback](#rollback-procedure) below |
| R2 | Minimum observability checks defined | **PASS** | See [Observability](#minimum-observability-checks) below |
| R3 | CI gates green on the release commit | **PASS** | `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (lint + test jobs green on #103-#110) |

## Rollback procedure

Single-instance StatefulSet, so rollback is a controlled replace, not a scale event:

1. **Pin forward and back by commit/tag + image digest.** Keep the previous release's chart values and image digest.
2. **Roll back the deployment**: `helm rollback <release> <previous-revision>` (or redeploy the previous chart/image). The StatefulSet terminates the old pod before starting the new one; the **preflight initContainer re-gates** the rollback.
3. **State-store compatibility**: the checkpoint/schema store is shared across versions. Roll back only to a version with a compatible on-disk format; if a release changed the checkpoint or schema-registry format, treat the state store as forward-only and follow that release's notes (a rollback may require restore from backup or re-snapshot).
4. **Verify** `/ready` returns 200 and the expected pipelines are `running` before restoring traffic.
5. **If the state volume is suspect**, restore from the last good backup (accept at-least-once re-delivery after the backup point) or re-snapshot.

## Minimum observability checks

Confirm before and after release:

- **Liveness**: `GET /health` returns 200 (process/event loop alive; stays 200 even if a pipeline is failed).
- **Readiness**: `GET /ready` returns 200 when all pipelines are healthy, 503 naming the offender when any required pipeline is `failed`.
- **Metrics**: `GET /metrics` (default `:9000`) serves Prometheus metrics, including `deltaforge_pipeline_status` per pipeline, source/sink counters, and stage-latency histograms. Restrict exposure per O2.
- **Pipeline status**: `deltaforge_pipeline_status` reflects running/paused/stopped/failed; alert on `failed` (it corresponds to `/ready` 503 and requires operator restart per D2).
- **Logs**: `RUST_LOG` set to an operable level; a failed pipeline logs a typed, actionable error (e.g. schema-drift-under-Halt remediation).

Evidence: `crates/rest-api/src/health.rs`, `crates/o11y/src/df_metrics.rs`, [Observability](../src/observability.md).

## Acceptance criteria (summary)

A release is **accepted** when:

1. C1-C6 are **PASS** (correctness + green matrix + soak).
2. R1-R3 are **PASS** (rollback, observability, CI gates).
3. Every ACCEPTED LIMITATION (D1-D2, O1-O2, O4, O6, S1-S2) is documented in the Supported Deployment Envelope with its operator control in place.
4. There are **zero BLOCKED** criteria.

Current status against this release baseline (#103-#110): **no BLOCKED criteria**; all correctness and release-engineering criteria PASS; the listed limitations are accepted with operator controls documented.
