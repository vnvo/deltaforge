# Release qualification runs (G3, G4)

Configurations and the launch recipe for the two qualification gates of the
[release checklist](../../../docs/ops/release-checklist.md): G3 (MySQL T1a)
and G4 (PostgreSQL source). Both run against the release image of the
candidate commit, on throwaway containers, with the **SQLite** state store.

- **Neither run qualifies the PostgreSQL state store.** Its qualification is
  deferred; `result.json` of G4 says so (`pg_state_store_qualified: false`).
- **Departure from the earlier T1a plan:** T1a was planned with the
  PostgreSQL state store. G3 runs it on SQLite, the supported production
  backend. Without a `measure.state_store_dsn`, the harness reads checkpoint
  progress through the REST API (`/pipelines/{name}/checkpoints`, positions
  only) and the store's size from `measure.state_store_file` (a bind mount of
  the instance's store).
- **Disk and time:** every invocation runs under
  [`floor-watch.sh`](floor-watch.sh): it starts with at least 50 GiB free,
  stops if free disk falls below 25 GiB, and stops at its deadline
  (`DEADLINE_SECS`). A stop sends SIGINT (the harness aborts and still writes
  its result), then SIGKILL after `GRACE_SECS` (default 120). `df -B1G` rounds
  up, so pass `START_GIB=51 FLOOR_GIB=26` to hold the floors exactly or
  stricter, and check the start in bytes (at least 53,687,091,200).
- **Deadlines:** warm-up + duration + `--drain-max-secs` + 900 s for setup and
  verification: S1 4,620 s; A1, A2, A3 4,020 s; S3 3,720 s. G4
  (`pg-qualify`): snapshot timeout + CDC + drain maximum + 900 s = 6,300 s. A stopped invocation is a failure of that
  invocation, kept like any other.
- **Failures** are kept and investigated, never rerun until they pass. A
  scenario that fails stops the sequence.

## Common

```bash
REV=$(git rev-parse --short HEAD)
git archive --format=tar HEAD | docker build -f Dockerfile -t deltaforge:qual-$REV -
cargo build --release -p fleet-harness
```

Images (pinned):

```
mysql@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084        (8.4)
postgres@sha256:a426e44bac0b759c95894d68e1a0ac03ecc20b619f498a91aae373bf06d8508d     (17)
confluentinc/cp-kafka@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41  (7.5.0)
confluentinc/cp-schema-registry:7.5.0                                               (G4 only)
```

The instance's log level must include the proof trace:
`RUST_LOG=warn,deltaforge::proof_trace=info`.

## G3: MySQL T1a

One network `g3`. MySQL as in the earlier attempts (3 CPU, 6 GiB):
`--server-id=101 --log-bin=mysql-bin --binlog-format=ROW
--binlog-row-image=FULL --binlog-row-metadata=FULL --gtid-mode=ON
--enforce-gtid-consistency=ON --log-replica-updates=ON
--binlog-expire-logs-seconds=21600 --max-binlog-size=268435456
--innodb-buffer-pool-size=2G --innodb-file-per-table=ON`, published on
`127.0.0.1:33061`, with a `df` user granted `REPLICATION SLAVE, REPLICATION
CLIENT` and `SELECT, SHOW VIEW` on `cust_%`. Kafka in KRaft mode (2 CPU,
3 GiB) with listeners `PLAINTEXT://g3-kafka:29092` and
`EXTERNAL://localhost:9395`, **topic auto-creation off** (the harness creates
the run's topics).

DeltaForge (4 CPU, 6 GiB), its store in the run directory:

```bash
mkdir -p state
docker run -d --name g3-deltaforge --network g3 --cpus 4 --memory 6g \
  -e RUST_LOG=warn,deltaforge::proof_trace=info \
  -v "$PWD/state:/data" -p 127.0.0.1:18080:8080 -p 127.0.0.1:19000:9000 \
  deltaforge:qual-$REV --storage-backend sqlite --storage-path /data/deltaforge.db
```

Then, from the run directory:

```bash
fleet-harness fixture g3-t1a-S1.yaml --concurrency 16
for sc in S1 A1 A2 A3 S3; do
  DEADLINE_SECS=<deadline of $sc> START_GIB=51 FLOOR_GIB=26 \
    floor-watch.sh -- fleet-harness run g3-t1a-$sc.yaml --drain-idle-secs 60 --drain-max-secs 1800 || break
done
```

S3 runs last: it changes the fixture.

## G4: PostgreSQL source

One network `g4`. PostgreSQL 17 with `-c wal_level=logical -c
max_replication_slots=10 -c max_wal_senders=10`, published on
`127.0.0.1:55432`, database `g4`, superuser `postgres`, and a role
`df` with `LOGIN REPLICATION` and `CONNECT` on `g4` (the harness grants it each
run's schema). Kafka as for G3 with listeners `PLAINTEXT://g4-kafka:29092` and
`EXTERNAL://localhost:9399`; a schema registry `g4-schema-registry:8081` (the
unsupported cases use an Avro sink). DeltaForge as for G3, named
`g4-deltaforge`, on `127.0.0.1:18081` (API) and `127.0.0.1:19001` (metrics),
with its own `state` directory.

```bash
DEADLINE_SECS=6300 START_GIB=51 FLOOR_GIB=26 floor-watch.sh -- fleet-harness pg-qualify g4-postgres.yaml
```

`results/g4/<run id>/` holds `result.json` (verdict and reasons),
`ledger.jsonl` (every committed change), `delivered.jsonl` (every decoded
event), `statuses.json` (the pipeline status, polled every 500 ms) and
`pipeline.json`. What G4 checks is in the release checklist; the
deliberately unsupported cases are reported under `unsupported_cases`, not as
failures, as long as each fails visibly as specified.
