# DeltaForge Chaos Tests

End-to-end resilience scenarios and benchmarks that run against a live Docker Compose stack. Each scenario injects a specific failure, asserts the expected outcome, and restores the stack to a clean state before the next scenario runs.

Two source backends are supported: **MySQL** and **PostgreSQL**. Generic scenarios (network partition, sink outage, crash recovery, schema drift) run against either source. Source-specific scenarios (failover, binlog purge for MySQL; pg_failover, slot_dropped for PostgreSQL) are wired to their respective source.

A **web UI playground** is also included for interactive fault injection, scenario control, and live DeltaForge API access without touching the command line.

## Prerequisites

- Docker with Compose V2
- The DeltaForge images built locally (the `df` profile uses all three):
  - `docker build -t deltaforge:dev .` (release)
  - `docker build --build-arg CARGO_PROFILE=dev -t deltaforge:dev-debug -f Dockerfile.debug .` (debug)
  - `docker build --build-arg CARGO_PROFILE=profiling -t deltaforge:dev-profile -f Dockerfile.debug .` (profile)
- Rust toolchain (to compile and run the scenario runner)

## Stack profiles

The compose file uses profiles so you bring up only the infra a given test needs. Nothing starts without a `--profile` flag — infra and app profiles are independent, so you can restart DeltaForge without touching the databases.

| Profile | Services | Use case |
|---------|----------|----------|
| `base` | toxiproxy, prometheus, grafana, cadvisor, loki, promtail | Fault injection + observability (always) |
| `mysql-infra` | mysql, mysql-b | MySQL source + failover replica |
| `pg-infra` | postgres, postgres-b | PostgreSQL source + failover replica |
| `kafka-infra` | kafka (KRaft, no Zookeeper), schema-registry | Kafka sink + Avro tests |
| `s3-infra` | minio, mc-bootstrap | S3/Parquet sink tests (MinIO) — see [S3_CHAOS.md](S3_CHAOS.md) |
| `df` | deltaforge-release, deltaforge-debug, deltaforge-profile | The three DeltaForge instances |

The `df` profile starts one container per build variant: release (`:8080`, metrics `:9000`), debug (`:8081`/`:9001`), profile (`:8082`/`:9002`). All start with a default config; the pipeline for each scenario is selected at runtime via the REST API (`DELETE` + `POST /pipelines`), **not** by profile or by separate per-source services.

## Stack setup

Start `base`, the infra you need, and `df`:

```bash
# MySQL resilience scenarios
docker compose -f docker-compose.chaos.yml \
  --profile base --profile mysql-infra --profile df up -d
curl http://localhost:8080/health   # should return "ok"

# PostgreSQL resilience scenarios
docker compose -f docker-compose.chaos.yml \
  --profile base --profile pg-infra --profile df up -d

# Kafka / Avro scenarios (add kafka-infra)
docker compose -f docker-compose.chaos.yml \
  --profile base --profile mysql-infra --profile kafka-infra --profile df up -d

# S3 / Parquet sink scenarios (add s3-infra)
docker compose -f docker-compose.chaos.yml \
  --profile base --profile mysql-infra --profile s3-infra --profile df up -d
```

Restart just the DeltaForge containers without touching infra:

```bash
docker compose -f docker-compose.chaos.yml --profile df restart
```

Grafana is available at `http://localhost:3000` (anonymous admin). Prometheus at `http://localhost:9090`.

## Playground UI

The chaos binary includes an interactive web UI that provides a live view of the Docker stack, one-click fault injection, scenario control, and a full DeltaForge REST API browser — all in the browser.

```bash
cargo run -p chaos -- --scenario ui
# Open http://localhost:7474
```

The UI has three areas:

- **Chaos tab** — service health grid with start/stop buttons per profile, fault injection (MySQL/PG/Kafka partition, latency, throttle), and the scenario runner with live log output.
- **Data Management** — reset checkpoints (clears stale GTID/offset state) or reset all volumes (`docker compose down -v`). Useful when switching branches or after binlog purge errors.
- **Pipeline API tab** — proxied access to any DeltaForge instance (port 8080, 8081, or 8082) for inspecting pipelines, schemas, sensing stats, and drift results.

Fault injection and scenario settings (duration, writer count, drain throughput knobs, rdkafka producer tuning) are configurable per run from the UI.

## Running scenarios from the CLI

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

# Long-running endurance (requires soak profile)
cargo run -p chaos -- --scenario soak --duration-mins 120 --writer-tasks 16
cargo run -p chaos -- --scenario soak-stable --duration-mins 60

# TPC-C benchmark (requires tpcc profile)
cargo run -p chaos -- --scenario tpcc --duration-mins 60

# Backlog-drain benchmark (requires soak profile)
cargo run -p chaos -- --scenario backlog-drain
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

### Endurance scenarios

#### `soak` (MySQL, requires `soak` profile)
Long-running endurance test against a 120-table MySQL workload. Concurrent writer tasks insert rows at a configurable rate while random faults (network partition, Kafka outage, latency injection) are injected every few minutes. Verifies DeltaForge keeps up and delivers all events correctly over time.

```bash
cargo run -p chaos -- --scenario soak \
  --duration-mins 120 \
  --writer-tasks 16 \
  --write-delay-ms 30
```

**What it proves:** sustained throughput, fault recovery under load, no silent data loss over hours.

#### `soak-stable` (MySQL, requires `soak` profile)
Same as `soak` but with no fault injection — writers and DDL operations run for the full duration against a stable stack. Use to establish baseline throughput, cache hit ratio, and resource usage numbers for comparison with the fault-injected run.

```bash
cargo run -p chaos -- --scenario soak-stable --duration-mins 60
```

**What it proves:** steady-state performance baseline (schema cache hit ratio, latency, CPU/memory).

#### `tpcc` (MySQL, requires `tpcc` profile)
TPC-C inspired endurance test using a 9-table wholesale-supplier schema (Warehouse, District, Customer, Order, Order-Line, Item, Stock, New-Order, History). Runs a New-Order / Payment / Delivery transaction mix for the configured duration. Measures CDC throughput against a realistic OLTP workload.

```bash
cargo run -p chaos -- --scenario tpcc --duration-mins 60
```

**What it proves:** CDC throughput and correctness under a realistic relational transaction mix.

### Benchmarks

#### `backlog-drain` (MySQL, requires `soak` profile)
Measures how quickly DeltaForge can replay a pre-built binlog backlog — the catch-up path that runs on cold starts, long outages, or migrations.

**Method:**
1. Stop the `chaos-soak` pipeline via the REST API so DeltaForge disconnects and saves its binlog checkpoint.
2. Write 1,000,000 rows to MySQL as fast as possible using 32 concurrent writers.
3. PATCH the pipeline with throughput settings (batch size, Kafka acks, schema sensing on/off) and resume from the saved checkpoint.
4. Poll the Kafka topic offset until all 1M events are delivered, then report drain throughput.

```bash
# Default settings (batch 200, sensing off, required acks)
cargo run -p chaos -- --scenario backlog-drain

# Tuned for higher throughput
cargo run -p chaos -- --scenario backlog-drain \
  --drain-max-events 3500 \
  --drain-max-ms 50 \
  --drain-commit-mode required

# With rdkafka producer tuning for maximum throughput
cargo run -p chaos -- --scenario backlog-drain \
  --drain-max-events 3500 --drain-max-ms 50 \
  --drain-kafka-conf batch.size=1048576 \
  --drain-kafka-conf linger.ms=20 \
  --drain-kafka-conf batch.num.messages=100000
```

**CLI flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--drain-max-events` | `200` | Max events per pipeline batch |
| `--drain-max-ms` | `100` | Max batch age in ms |
| `--drain-commit-mode` | `required` | Commit mode: `required` (safe) or `periodic` (faster) |
| `--drain-schema-sensing` | `false` | Enable schema sensing during drain (slower) |
| `--drain-kafka-conf KEY=VALUE` | *(none)* | rdkafka producer overrides (repeatable) |

**Useful `--drain-kafka-conf` keys:**

| Key | rdkafka default | Description |
|-----|-----------------|-------------|
| `batch.size` | `16384` (16 KB) | Max bytes per Kafka produce request. Raise to 1 MB+ for high-throughput drains. |
| `linger.ms` | `5` | How long rdkafka waits to fill a batch before sending. Higher = more batching. |
| `batch.num.messages` | `10000` | Max messages per Kafka produce request. |
| `compression.type` | `lz4` | Compression codec: `lz4`, `snappy`, `zstd`, `none`. |

These settings are also available in the **Playground UI** under the "Kafka Producer Tuning" panel when the backlog-drain scenario is selected.

**Output includes:** write throughput, drain duration, avg/p50/peak drain events/s, and any kafka overrides applied.

**What it proves:** binlog catch-up throughput, checkpoint resume correctness, impact of batch size, rdkafka producer tuning, and schema sensing on replay performance.

> Schema sensing processes every event to infer structure and is ~15x slower than disabled. Disable it (the default) for maximum drain throughput. The original pipeline config is restored on container restart.

## Network topology

```
The three DeltaForge instances differ by build variant, not by source. Each
runs whatever pipeline config is pushed to it, routing through Toxiproxy:

DeltaForge (release/debug/profile) ──► Toxiproxy ──► mysql            (proxy 5100)
                                                 ──► postgres         (proxy 5101)
                                                 ──► kafka            (proxy 5102)
                                                 ──► schema-registry  (proxy 5103)
                                                 ──► minio (S3)       (proxy 5104)

Toxiproxy API:          localhost:8474  (scenario runner injects faults here)
DeltaForge (release):   localhost:8080  (health/REST API · metrics :9000)
DeltaForge (debug):     localhost:8081  (health/REST API · metrics :9001)
DeltaForge (profile):   localhost:8082  (health/REST API · metrics :9002)
Kafka (direct):         localhost:9092  (scenario runner reads offsets here)
MySQL (direct):         localhost:3306  (scenario runner inserts rows here)
Postgres (direct):      localhost:5432  (scenario runner inserts rows here)
MinIO (direct):         localhost:9100  (S3 API · console :9101)
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
docker compose -f docker-compose.chaos.yml --profile pg --profile pg-app down -v
docker compose -f docker-compose.chaos.yml down -v
```

### Soak / TPC-C
```bash
docker compose -f docker-compose.chaos.yml --profile soak down -v
docker compose -f docker-compose.chaos.yml --profile tpcc down -v
docker compose -f docker-compose.chaos.yml down -v
```

The `-v` flag removes all volumes, giving the next run a completely clean slate.

### Reset from the Playground UI

The UI's **Data Management** card provides two reset options without touching the command line:

- **Reset Checkpoints** — stops all DeltaForge instances and deletes their SQLite checkpoint databases (GTID positions, replication slot offsets). Source databases and Kafka data are preserved. This is the quickest fix for stale checkpoint errors (e.g. "binlog position purged" after switching branches).
- **Reset All Volumes** — equivalent to `docker compose down -v` across all profiles. Wipes everything including MySQL data, Kafka state, and Grafana dashboards.

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
