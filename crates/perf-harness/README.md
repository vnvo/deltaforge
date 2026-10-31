# perf-harness (`canonical-perf`)

Performance comparison tooling: an interleaved A/B of two systems on
identical, deterministic MySQL and PostgreSQL fixtures, delivering to Kafka
(JSON or Avro) or S3 (Parquet). A system is a DeltaForge release binary or
Debezium on Kafka Connect. This is measurement tooling, not a test: it is
not part of the gate and changes no product behavior.

Results: [`results/2026-10-debezium-comparison.md`](results/2026-10-debezium-comparison.md)
(DeltaForge against Debezium, including the runs found invalid and why).

## What a run measures

Each run starts a fresh DeltaForge process, or registers a connector on a
shared, warmed-up Kafka Connect worker. It is measured from outside over the
delivery window:

- **Throughput:** a paused pipeline is resumed onto a backlog (CDC), or a
  table is copied (snapshot), until the last row arrives.
- **Latency:** commit to arrival at fixed transaction rates, transaction
  sizes and bursts.
- **Resources:** CPU and system time, peak RSS, and per-thread on-CPU and
  runqueue time, context switches and migrations.
- **Delivery counters:** batches, checkpoint writes and Kafka requests, and
  output bytes.

Repetitions alternate the two systems, so drift on the host affects both.

## Pinned inputs

All images are pinned by digest in `src/main.rs` and `src/debezium.rs`:

- MySQL 8.4, PostgreSQL 17, Confluent Kafka and Schema Registry 7.5.0;
- `quay.io/debezium/connect:3.6.3.Final` (Kafka Connect 4.3, OpenJDK 25).

The Debezium worker adds plugins its image does not ship. They are fetched
from official distributions and verified against the SHA-256 digests in
`src/extras.rs`:

- Confluent's Avro converter 8.3.2 (Confluent Hub);
- Confluent's S3 sink connector 12.1.13 (Confluent Hub), Debezium's path to
  Parquet;
- GraalJS 25.4.4.1.1 and its runtime jars (Maven Central), the engine of
  Debezium's scripting transformations.

## Safety and identity checks

- One invocation per host at a time (a lock file).
- A run refuses to start while anything holds the runner's API or metrics
  port, or answers on it.
- The process serving the API must be the runner just started, or one it
  runs (for example under a profiler).
- A run that stops delivering for 120 s fails instead of waiting. A Kafka
  Connect worker that stops answering is killed, so it takes no CPU from
  later runs.

## What an invocation records

- **`invocation.json`:**
  - its arguments;
  - every pinned image and artifact;
  - the harness executable's SHA-256, and the revision and cleanliness of
    the directory it ran in;
  - the host (CPU model, kernel, CPU count);
  - for a layout comparison, every link.
- **`runs.jsonl`:** one line per run, including:
  - the runner's PID, the executable it ran and its SHA-256;
  - every measurement, the per-thread counters and the delivery counters.
- **Per run under `runs/`:**
  - the exact pipeline spec (`pipeline.json`) or Debezium connector
    configuration (`connector.json`);
  - the logs.
- **`connect.log`:** the Kafka Connect worker's log.
- **`summary.md`:** medians per cell, each cell's comparison class, and the
  references to the raw results.

## Comparison classes

Against Debezium a cell is either:

- **direct:** both systems run the same workload to equivalent output. The
  JSON and Avro CDC, snapshot, latency, catch-up, restart, crash and DDL
  cells are direct. Their envelopes differ in bytes, which is reported. So
  are the JavaScript filtering and routing cells (`-jsroute`): both keep the
  rows with an even id and route each to one of two topics by `id % 4`
  (DeltaForge's JavaScript processor; Debezium's `Filter` and
  `ContentBasedRouter`). The consumer checks every row's topic, and a row
  that should have been filtered out or arrives on the wrong topic fails
  the run.
- **architectural:** the systems do the work in different ways, and
  `summary.md` states how. The JavaScript cells are architectural.
  DeltaForge runs a batch JavaScript processor that adds a field.
  Debezium's scripting transformations filter or route records and cannot
  add one, so its cell evaluates a per-record JavaScript condition instead.
  On its image's OpenJDK that condition runs on GraalJS's interpreter: there
  is no optimizing Truffle runtime.

DeltaForge against DeltaForge is always direct.

## Parquet end to end

DeltaForge writes Parquet to S3 directly: its durable S3 sink writes one
object per 2,000-row batch. Debezium has no S3 path of its own. It writes
Avro to Kafka, and Confluent's S3 sink connector 12.1.13 (Confluent Hub,
pinned in `src/extras.rs`) writes the topic to the same RustFS server as
Parquet. That connector is configured to match: 2,000 rows per object
(`flush.size`), and a partial object every second
(`rotate.schedule.interval.ms`). The cell is architectural: the paths
differ, and so do the columns (DeltaForge's flat envelope, Debezium's
nested one).

The connector checks its rotation only when records arrive, or at the
worker's offset commit (60 s). The last partial object of a backlog lands
on that commit. Two times are recorded:

- `secs`: until every row is visible;
- `bulk_s`: until all but the last object's 2,000 rows are visible.

Each run also records `objects`, the number of Parquet objects written.

## Crash and recovery

The crash cells kill the system mid-backlog: SIGKILL for DeltaForge,
`docker kill` for the Kafka Connect worker. Completion is by distinct row
id, and a replayed row counts as a duplicate. Each system first has a
durable position. DeltaForge has checkpointed the marker row. Debezium is
crashed only after its worker has committed an offset for the connector
(`offset.flush.interval.ms`, 60 s by default). Without one, a `no_data`
connector restarts at the binary log's end, past every row it had not
delivered.

After the kill, DeltaForge's store gate is still held. By design it does
not expire. The harness performs the operator step: `store-gate status`,
a check that the recorded process is gone, then `store-gate break
--owner`. The run records it (`gate_broken`) and counts it in the
recovery time. Recorded per run:

- `restart_s`: kill to the restarted system serving again;
- `recovery_s`: kill to the first row it delivers;
- CPU, including the killed process's CPU up to the kill.

What a crash replays depends on how often each system records its position.
DeltaForge checkpoints after each delivered batch, so a crash replays at
most the batch in flight. Debezium's worker commits offsets on
`offset.flush.interval.ms` (60 s by default), and a crash replays what was
delivered since the last commit. `--debezium-offset-flush-ms` sets that
interval for a run. With `--delivery-stats`, the cost of a shorter interval
shows in the offset records and Kafka requests.

## Per-instance scaling study

`--scale-tables` runs the scaling study (`src/scale.rs`) instead of the cell
matrix. One DeltaForge MySQL source and one Debezium connector cover a
fixture of many tables, of which a share changes. The design, fixed before
any result, is `results/scale-study-design.md`. It covers fixtures, active
sets and rotation, workload modes, the declared capacity limits, the binary
log's controlled starting offset, and what each run records.

```bash
canonical-perf --binary df=<runner> --binary dbz=debezium --debezium-profile tuned \
    --out target/perf/scale --reps 3 --scale-tables 100,1000 \
    --scale-active 1,20,50,100 --scale-workload steady,burst
```

Options:

- `--scale-rotate` rotates the active sets.
- `--scale-binlog-mib` sets the binary log's starting offsets, and
  `--scale-binlog-pad small|large` what pads the file up to them.
- `--scale-catchup false` drops the catch-up phase.
- `--scale-seconds` sets the load's length.
- `--scale-points` runs only the listed points of the grid (ids as in
  `scale-summary.md`). An id outside the grid, or one listed twice, is
  refused. The points run in the grid's order, whatever the list's order.
  It is how a point is re-run with more repetitions in one invocation. The
  stage-1 expansion to five repetitions was:

  ```bash
  canonical-perf --binary df=<runner> --binary dbz=debezium --debezium-profile tuned \
      --out target/perf/scale-stage1x --reps 5 --scale-tables 100,1000 \
      --scale-active 1,20,50,100 --scale-workload steady,burst --scale-binlog-mib 0 \
      --scale-points t100-a1-steady-o0,t100-a20-burst-o0,t100-a50-burst-o0,t100-a50-steady-o0,t100-a100-burst-o0,t1000-a1-burst-o0,t1000-a50-steady-o0,t1000-a100-steady-o0
  ```

The load-generation settings apply equally to both systems and are not
production recommendations: 64 writer connections, and a source that does
not sync its logs.

Each run records:

- correctness (missing, duplicated, out-of-order rows, and rows from
  outside the run's ids or of an unknown table);
- startup, catch-up and drain times;
- steady, activation (first row per table) and catch-up latency, and after
  the catch-up resume the latency of tables activated before the stop;
- selected, touched and activated tables, and DeltaForge's windows, plans,
  proofs and their seek, from its proof trace;
- resources, MySQL and Kafka counters; DeltaForge's connections by kind and
  selection-cache accounting, sampled every 250 ms;
- its place in the invocation, and MySQL's data directory size before and
  after;
- its raw commit and arrival times (`rows.csv`).

Stage 2's rules (`results/scale-stage2-plan.md`) apply to every run:

- **Drain:** after MySQL's final `gtid_executed` is read, one marker row is
  committed (Debezium's offset stops before the last transaction it
  processed). Every committed row and the marker arrived and the durable
  frontier covers that GTID set (DeltaForge's sink checkpoints; Debezium's
  committed offset, after the connector is stopped), then the system is
  shut down and the topic read to its end. A timeout fails the run.
- **One system:** the scaling study also runs with a single `--binary`.
- **Measured failures:** permit timeouts, saturation deferrals, window
  deferrals or overflows, failed proof rounds and capture-kill failures
  fail a run, which is kept with its cause.
- **Resource floors:** 25 GiB free disk (the output's and Docker's) and
  4 GiB of available memory, sampled every second into `monitor.jsonl`. A
  breach cancels the fixture build or run in progress, records it as failed
  and ends the invocation.
- **Store:** with `--state-store postgres` each DeltaForge run gets a
  database of its own, and reports its store statements and commits.
- **Proof trace:** a DeltaForge run needs
  `PERF_RUST_LOG=warn,deltaforge::proof_trace=info`; the study refuses to
  start without it.

An interrupt (Ctrl-C or SIGTERM) stops the runner and removes the
invocation's containers and their volumes. `PERF_RUST_LOG` sets the
runners' log filter for diagnostics, and is recorded in `invocation.json`.

Results: `results/2026-10-proof-seek-sensitivity.md`.

## Layout-randomized qualification

A single link of a version can run a few percent faster or slower than
another link of the same source, depending on where its functions land. On
the measurement host this moved an unchanged release by 2.5%, more than a
2% budget. Performance qualification therefore compares versions across
several links:

```bash
crates/perf-harness/layout-links.sh <base-rev> /tmp/links-base
crates/perf-harness/layout-links.sh <new-rev>  /tmp/links-new
canonical-perf --layout base=/tmp/links-base --layout new=/tmp/links-new \
    --out target/perf/qualify --reps 5 [--filter ...]
```

The protocol:

- **Seeds:** at least five links per version, with the fixed seeds in
  `layout-links.sh` (101, 202, 303, 404, 505). They are declared before any
  result. A comparison refuses versions linked with different seeds.
- **Identity:** every link's SHA-256 is recorded in its version's
  `manifest.json`, and re-checked before the run.
- **Workload:** for each seed, both versions run the same workload in the
  same order, interleaved, with several repetitions, on the same host.
- **Report:** `layout-summary.md` and `layout.json` aggregate every seed:
  - per cell, the pooled geometric mean of paired ratios with a 95%
    bootstrap interval;
  - each seed's geometric mean and the range across seeds;
  - the executables' hashes.
- **No exclusions:** every seed counts. A failed or missing run is listed as
  an incomplete pair, never dropped.

Layout randomization belongs to performance qualification and release
evidence. The ordinary gate tests the exact release candidate.

## Running

```bash
cargo build --release -p perf-harness
target/release/canonical-perf --list
target/release/canonical-perf --binary base=<runner> --binary new=<runner> \
    --out target/perf/ab --reps 5 --filter =my-cdc-json-narrow
target/release/canonical-perf --binary df=<runner> --binary dbz=debezium \
    --debezium-profile tuned --delivery-stats --out target/perf/dbz --reps 5
```

Other options:

- `--delivery-stats` reads the delivery counters (it adds 5.5 s per run);
- `--df-batch` and `--df-sink LABEL=JSON` add batch or sink settings to
  DeltaForge pipelines;
- `--scale` shrinks every cell for a smoke run.

The harness needs Docker, and it starts and removes its own containers.
