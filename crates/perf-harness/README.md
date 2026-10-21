# perf-harness (`canonical-perf`)

Performance comparison tooling: an interleaved A/B of two systems on
identical, deterministic MySQL and PostgreSQL fixtures, delivering to Kafka
(JSON or Avro) or S3 (Parquet). A system is a DeltaForge release binary or
Debezium on Kafka Connect. This is measurement tooling, not a test: it is
not part of the gate and changes no product behavior.

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
  JSON and Avro CDC, snapshot and latency cells are direct. Their envelopes
  differ in bytes, which is reported.
- **architectural:** the systems do the work in different ways, and
  `summary.md` states how. The JavaScript cells are architectural.
  DeltaForge runs a batch JavaScript processor that adds a field.
  Debezium's scripting transformations filter or route records and cannot
  add one, so its cell evaluates a per-record JavaScript condition instead.
  On its image's OpenJDK that condition runs on GraalJS's interpreter: there
  is no optimizing Truffle runtime.

DeltaForge against DeltaForge is always direct.

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
