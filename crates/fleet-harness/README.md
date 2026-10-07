# fleet-harness

Tooling for the single-instance MySQL fleet qualification
([design](../../docs/design/mysql-fleet-qualification.md)). It measures what one
DeltaForge instance achieves with many large MySQL clusters. It does not
decide a capacity, and it does not change DeltaForge.

- **Profile:** CDC-qualified only. The full-cluster snapshot (S6) belongs to the
  snapshot profile and is refused.
- **Source counts are hypotheses:** a run sweeps the configured counts (for
  example 1, 25, 50, 100, 300). It never assumes a count succeeds, and it stops
  at the first count that fails correctness.

## Exploratory and qualification results

Every value the owner has not supplied is a **placeholder**: a bare value in the
configuration. Only `{ value: ..., provenance: owner }` is an owner input.

| | Exploratory | Qualification |
|---|---|---|
| Placeholders | allowed | refused (`fleet-harness check` lists them) |
| State store | any | `postgres` only |
| Keys | any | `verifier.expected_key: primary_key` (primary-key keys on every operation, deletes included) |
| Short scenarios | any repetitions | at least 5 |
| Results | `<output_dir>/exploratory/` | `<output_dir>/qualification/` |
| `claims_allowed` | always `false` | `true` |
| Budgets | reported as "placeholder: not evaluated" | evaluated (the worst measured run must pass) |
| Safe source count | never declared | the largest count whose every run kept correctness and met every budget |

A result records what was achieved, not only pass or fail:
- committed operations per second per cluster;
- end-to-end lag p50/p90/p99 per cluster;
- process cgroup memory (peak and mean) and CPU, threads and file descriptors;
- MySQL connections per server;
- state-store size;
- recovery to 50%, 90% and 100% of sources after each disruption;
- metrics scrape time and size.

## What a repetition must achieve

A sweep counts a repetition only if `verdict.repetition_ok` holds.

- **Exploratory:** correctness. Everything else is recorded.
- **Qualification:**
  - **correctness;**
  - **every action of the plan ran without error** (a missing hook or a failed
    action fails the repetition);
  - **no uncertain transaction**, unless the owner allows some with
    `policy.max_uncertain_transactions`, and no writer ended early;
  - **the workload was achieved:** per server, committed operations over the
    configured target (integrated over the peak schedule and any rate change) of
    at least `policy.min_achieved_ratio`, an owner input. Losing writers or
    tables cannot pass as capacity.

  Budgets are then judged by the sweep.

## Commands

```bash
# What is still a placeholder, and what blocks a qualification run.
cargo run -p fleet-harness -- check crates/fleet-harness/configs/t1-exploratory.yaml

# Build (or resume, or reuse) the fixture: databases x tables per server.
cargo run --release -p fleet-harness -- fixture crates/fleet-harness/configs/t1-exploratory.yaml --concurrency 16

# The pipeline specs a run creates (one per cluster).
cargo run -p fleet-harness -- render crates/fleet-harness/configs/t1-exploratory.yaml

# Run the configured scenario at every source count and repetition.
cargo run --release -p fleet-harness -- run crates/fleet-harness/configs/t1-exploratory.yaml

# One cell of the physical state-store matrix, on an empty PostgreSQL store.
cargo run --release -p fleet-harness -- store-matrix --dsn postgres://... \
  --sources 1 --tables-per-source 400000 --versions-changed 10 --changed-fraction 0.1
```

## How correctness is checked

- **Identity never comes from the message key.** The verifier takes a row from
  `after`, or from `before` for deletes.
- **Completeness and order are checked offline.** Every committed operation
  (driver) and every consumed event (verifier) is spilled to disk. The final
  check sorts both sides externally, so memory stays bounded at any scale. It
  checks:
  - completeness, final state, and duplicates (reported);
  - order within a partition (a failure);
  - cross-partition reorderings (reported).
- **The expectations are exact by construction:**
  - one writer per table;
  - one version sequence per server;
  - deletes stamped with an update in the same transaction;
  - row ids namespaced per run, so events of other runs on the same topics are
    skipped as foreign.
- **Failed transactions are reconciled against MySQL.** Each table has a single
  writer, so after a failed transaction the driver reads its rows back:
  - committed operations go to the ledger;
  - rolled-back ones have their row ranges undone.

  Only a transaction whose outcome cannot be determined is uncertain: the server
  was unreachable until the run stopped, or its rows contradict each other. It
  goes to an uncertain ledger, is neither missing nor unexpected, and its tables
  are retired. All of these are counted per server: driver errors, reconciled
  commits and rollbacks, uncertain transactions and operations, retired tables.
- **The sort is bounded in memory and open files.** Runs are merged at most
  `sort_fan_in` (default 64) at a time, in as many passes as needed. A truncated
  record is an error, never a silent end of file.
- **Migrations are verified with probe rows.** After every `ALTER TABLE`, a
  probe row must arrive with the new column's value.
- **Primary-key keys.** With `expected_key: primary_key`, every event must carry
  the row's primary key as its message key. Today a `${after.id}` template does
  this for inserts and updates but not deletes, as the smoke test records. The
  check is ready for the coalescing key the owner requires.

## Scenarios

S1-S17 (not S6) and A1-A5, as in the design. Soaks carry repeated disruptions
inside the run. Built-in actions:
- Toxiproxy endpoint outages;
- REST lifecycle operations (stop, resume, patch, restart);
- rate changes for hot clusters.

Environment-specific actions are command hooks under `actions:` in the
configuration. `{server}`, `{pipeline}` and `{pid}` are substituted.

| Hook | Used by |
|---|---|
| `endpoint_down`, `endpoint_up` | S11, soaks (when no Toxiproxy) |
| `sink_outage_start`, `sink_outage_stop` | S9, soaks |
| `store_restart` | S10 |
| `failover` | S7 |
| `rotate_credentials` | S12 |
| `lineage_barrier` | S8 |
| `process_stop`, `process_start` | S15, S17 |

An action whose hook or proxy is missing is recorded as **not run** in the
result, never silently skipped.

## Not covered yet

Each result records these:
- **State-store matrix:** activation histories and barrier streams, and
  byte-equivalent bulk fixtures for 10M+ tables. Their encoders are internal to
  the product crates, so they need a test-support entry point, which is a
  separate reviewed change.
- **Final qualification runs** need the before/after-coalescing key (owner
  requirement) and the owner's traffic, DDL and budget inputs.

## Tests

- Unit tests: `cargo test -p fleet-harness --lib`.
- Smoke test, which needs Docker (MySQL 8.4, Kafka, and DeltaForge in-process
  behind its REST API):

  ```bash
  cargo test -p fleet-harness --test harness_smoke -- --include-ignored --test-threads=1
  ```
