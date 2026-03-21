# DeltaForge Chaos Tests

End-to-end resilience scenarios that run against a live Docker Compose stack. Each scenario injects a specific failure, asserts the expected outcome, and restores the stack to a clean state before the next scenario runs.

Two source backends are supported: **MySQL** and **PostgreSQL**. Generic scenarios (network partition, sink outage, crash recovery, schema drift) run against either source. Source-specific scenarios (failover, binlog purge for MySQL; pg_failover, slot_dropped for PostgreSQL) are wired to their respective source.

## Prerequisites

- Docker with Compose V2
- The debug image built locally: `docker build -t deltaforge:dev-debug -f Dockerfile.debug .`
- Rust toolchain (to compile and run the scenario runner)

## Stack setup

### MySQL

```bash
docker compose -f docker-compose.chaos.yml up -d
docker compose -f docker-compose.chaos.yml --profile app up -d
curl http://localhost:8080/health   # should return "ok"
```

### PostgreSQL

```bash
docker compose -f docker-compose.chaos.yml up -d
docker compose -f docker-compose.chaos.yml --profile pg-app up -d
curl http://localhost:8080/health   # should return "ok"
```

> **Note:** `app` and `pg-app` both bind to ports 8080 and 9000. Bring up one at a time.

Grafana is available at `http://localhost:3000` (anonymous admin). Prometheus at `http://localhost:9090`.

## Running scenarios

```bash
# MySQL (default)
cargo run -p chaos -- --scenario all
cargo run -p chaos -- --scenario all --source mysql

# Individual MySQL scenarios
cargo run -p chaos -- --scenario network-partition
cargo run -p chaos -- --scenario sink-outage
cargo run -p chaos -- --scenario crash-recovery
cargo run -p chaos -- --scenario failover
cargo run -p chaos -- --scenario schema-drift
cargo run -p chaos -- --scenario binlog-purge

# PostgreSQL
cargo run -p chaos -- --scenario all --source postgres

# Individual PostgreSQL scenarios
cargo run -p chaos -- --scenario network-partition --source postgres
cargo run -p chaos -- --scenario pg-failover --source postgres
cargo run -p chaos -- --scenario slot-dropped --source postgres
```

The runner prints a summary at the end:

```
═══════════════════════════════════
  Chaos Run Results
═══════════════════════════════════

[PASS ✓] mysql/network_partition
[PASS ✓] mysql/sink_outage
[PASS ✓] mysql/crash_recovery
[PASS ✓] mysql/failover
[PASS ✓] mysql/schema_drift
[PASS ✓] mysql/binlog_purge

6/6 scenarios passed
```

Exit code is `0` on full pass, `1` if any scenario fails — suitable for CI.

## Scenarios

### Generic (run with any source via `--source mysql|postgres`)

#### `network_partition`
Cuts the source DB proxy via Toxiproxy, inserts rows directly into the DB while DeltaForge is disconnected, then restores connectivity. Verifies DeltaForge reconnects and delivers all rows without gaps. Runs 2 rounds.

**What it proves:** reconnect backoff, position-based resume, no event loss during partition.

#### `sink_outage`
Cuts the Kafka proxy, inserts rows while the sink is unreachable, then restores it. Verifies DeltaForge buffers events and flushes them once Kafka is back. Runs 2 rounds.

**What it proves:** sink retry, back-pressure handling, at-least-once delivery guarantee.

#### `crash_recovery`
Sends `SIGKILL` to the DeltaForge process (bypassing graceful shutdown), inserts more rows while it is restarting, then waits for full recovery. Verifies all post-crash events arrive and no events are duplicated.

**What it proves:** crash-safe checkpoints, Docker `restart: on-failure` recovery, deduplication.

#### `schema_drift`
Issues `ALTER TABLE customers ADD COLUMN notes TEXT` while DeltaForge is actively streaming. Inserts rows using the new schema and verifies they arrive in Kafka. Confirms DeltaForge remains healthy throughout. Drops the column on exit.

**What it proves:** DDL event handling, schema reload, `on_schema_drift=adapt` behaviour.

### MySQL-specific (`--source mysql`)

#### `failover`
Switches the MySQL Toxiproxy upstream from `mysql` (server A) to `mysql-b` (server B, different UUID) mid-stream. Verifies DeltaForge detects the server identity change, determines the checkpoint position is unreachable on the new server, and halts — reflected as a `503` on `/health`.

**What it proves:** server UUID guard, `check_position_reachability`, `/health` failure propagation.

#### `binlog_purge`
Stops DeltaForge cleanly (checkpoint saved), then runs `RESET BINARY LOGS AND GTIDS` to wipe all binlog history and GTID state. Restarts DeltaForge and verifies it halts rather than silently resuming from an invalid position — reflected as a `503` on `/health`.

**What it proves:** GTID reachability check on reconnect to same server, position-lost guard.

> **Note:** `binlog_purge` always runs last in `--scenario all --source mysql` because it wipes MySQL GTID state. The scenario cleans up by removing the stale checkpoint DB before restarting.

### PostgreSQL-specific (`--source postgres`)

#### `pg_failover`
Switches the PostgreSQL Toxiproxy upstream from `postgres` (server A) to `postgres-b` (server B, different `system_identifier`) mid-stream. Verifies DeltaForge detects the identity change, finds the replication slot missing on the new server, and halts — reflected as a `503` on `/health`.

**What it proves:** `system_identifier` guard, slot reachability check, `/health` failure propagation.

#### `slot_dropped`
Stops DeltaForge cleanly (checkpoint saved), then drops the replication slot directly on Postgres. Restarts DeltaForge and verifies it halts rather than silently resuming from an invalid position.

**What it proves:** replication slot existence guard, position-lost detection, parity with MySQL `binlog_purge`.

> **Note:** `slot_dropped` always runs last in `--scenario all --source postgres`. The scenario clears the checkpoint DB so the next run starts fresh.

## Network topology

```
DeltaForge ──► Toxiproxy ──► mysql          (port 5100)
                         ──► mysql-b         (MySQL failover target)
                         ──► postgres        (port 5101)
                         ──► kafka           (port 5102)

Toxiproxy API:   localhost:8474   (scenario runner injects faults here)
DeltaForge API:  localhost:8080   (health checks)
Kafka (direct):  localhost:9092   (scenario runner reads offsets here)
MySQL (direct):  localhost:3306   (scenario runner inserts rows here)
Postgres (direct): localhost:5432 (scenario runner inserts rows here)
```

`postgres-b` has no host port — it is only reachable via the Toxiproxy upstream switch.

## Teardown

### MySQL stack
```bash
docker compose -f docker-compose.chaos.yml --profile app down -v
docker compose -f docker-compose.chaos.yml down -v
```

### PostgreSQL stack
```bash
docker compose -f docker-compose.chaos.yml --profile pg-app down -v
docker compose -f docker-compose.chaos.yml down -v
```

The `-v` flag removes all volumes, giving the next run a completely clean slate.

## Adding a scenario

### Generic scenario (works with any source)
1. Create `crates/chaos/src/scenarios/<name>.rs` with signature `pub async fn run<B: SourceBackend>(harness: &Harness, backend: &B) -> Result<ScenarioResult>`. Use `backend.insert_rows()`, `backend.proxy()`, etc. for source-specific operations.
2. Register it in `crates/chaos/src/scenarios/mod.rs`.
3. Add a `Scenario::<Name>` variant and match arms in both `run_mysql` and `run_postgres` in `crates/chaos/src/main.rs`.

### Source-specific scenario
1. Create `crates/chaos/src/scenarios/<name>.rs` with a plain `pub async fn run(harness: &Harness) -> Result<ScenarioResult>`. Access the DB directly using `MYSQL_DSN` or `PG_DSN` from `crate::backend`.
2. Register it in `crates/chaos/src/scenarios/mod.rs`.
3. Add a `Scenario::<Name>` variant, a match arm in the relevant `run_mysql`/`run_postgres` function, and an error arm in the other function.
4. Place it before the destructive last scenario (`binlog_purge` / `slot_dropped`) in the `All` sequence.
