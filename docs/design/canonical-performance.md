# Canonical-Value Performance Comparison

The canonical event model (#153, merge `afeb56c`) changed how every row value is decoded, carried and rendered. This document records its performance against the exact pre-change parent (`d44d0be`) on identical release builds and fixtures, and the diagnosis of every difference larger than about 10%.

## 1. Method

**Builds.** Both revisions are built with `cargo build --release -p runner` from clean worktrees, each into its own target directory, using the toolchain pinned by the repository. The binary under test is the server (`runner`, jemalloc allocator).

**Harness.** `crates/perf-harness` (`canonical-perf`) measures both binaries from outside the process. The binaries share nothing but the pipeline configuration format. Each run:

- starts a fresh process with its own SQLite state store;
- creates one pipeline through the REST API;
- measures one cell;
- deletes the pipeline;
- stops the process with SIGTERM.

Repetitions alternate the order of the two versions, so drift on the shared machine affects both alike. Reported values are medians.

**Infrastructure.** All on one host (12 cores, 32 GB):

- MySQL 8.4 and PostgreSQL 17, pinned by digest;
- Kafka 7.5.0 and Schema Registry 7.5.0 (KRaft, one broker, one partition per topic);
- RustFS (the pinned S3 test server).

DeltaForge, the databases, Kafka and the harness share the CPUs. The comparison is relative: both versions run under the same conditions.

**Fixtures.** Rows are deterministic: the same SQL for both versions and every repetition.

| Shape | Columns | Rows |
|---|---|---|
| narrow | INT, VARCHAR, DECIMAL(12,3), TIMESTAMP(6) | 200,000 |
| wide | 60 columns cycling INT, BIGINT, VARCHAR(64), DECIMAL(12,3), DOUBLE, DATE, DATETIME(6) / timestamp | 50,000 |
| binary | 4 BLOB / bytea of 256 bytes | 50,000 |
| temporal | 20 columns cycling DATE, TIME(6), DATETIME(6) / timestamp, TIMESTAMP(6) / timestamptz | 100,000 |
| null-heavy | the wide schema with 90% NULLs | 50,000 |
| ints N | N INT columns (N = 10, 50, 200): the row-width sweep | 50,000 |

Rows are written 500 per statement, so each source transaction holds 500 rows. PostgreSQL tables use `REPLICA IDENTITY FULL`.

**Modes.**

- **CDC (backlog drain).** Streaming is first confirmed with marker rows. The pipeline is then paused (sources included), the N rows are written, and the clock starts when the pipeline resumes. It stops when the last row reaches the sink.
- **Snapshot.** N rows are preloaded. The clock starts at pipeline creation and stops when the last row reaches the sink, so it includes pipeline startup (about the same for both versions).
- **Latency.** 2,000 single-row transactions, one every 5 ms on one connection. Each row's latency is from its commit returning to its arrival at the Kafka consumer (Kafka sinks only; Parquet latency is dominated by object batching).

**Sinks.** Kafka JSON (native envelope), Kafka Avro (Schema Registry, DDL-derived schemas) and S3 Parquet (`durable_v2`), all with default batching. Parquet snapshot cells use 1,000-row snapshot chunks and a 1 s batch timer: with the defaults, both versions failed with "durable_v2 batch is missing its durable watermark", a product defect in how batches cut snapshot chunks, found by this run and fixed separately (`fix-durable-snapshot-chunks`). The batch-size cells set `batch.max_events` to 256 and 4096. JavaScript cells add one processor, which sets a field on every row.

**Measurements over the delivery window.**

- throughput: rows / window;
- process CPU (`utime + stime`), reported per 100k rows;
- peak RSS (sampled `VmRSS`);
- allocations: not available in the release build (its jemalloc has no statistics), so section 3 reads allocation pressure from CPU profiles. Section 4 counts allocation requests with builds that add `--features tikv-jemallocator/stats` (jemalloc's `nrequests` at exit, minus an idle run's);
- output bytes per row: Kafka payloads and keys, or S3 Parquet data objects;
- database CPU: the database container's cgroup `usage_usec`;
- for snapshots, the window is the snapshot duration;
- for latency cells, p50 and p99.

**Reproduce.**

```bash
cargo build --release -p runner   # in a worktree of each revision, separate CARGO_TARGET_DIR
cargo run --release -p perf-harness -- \
  --binary base=<d44d0be>/release/runner --binary canonical=<afeb56c>/release/runner \
  --out target/perf/<run> --reps 3
```

## 2. Results

### 2.1 Interleaved rerun (the reference numbers)

14 cells, one per regression class, 3 repetitions with alternating version order (`target/perf/rerun1`). Medians; CPU is process CPU per 100k rows; bytes are Kafka payload plus key (or Parquet objects) per row.

| Cell | Base rows/s | Canonical rows/s | Ratio (range) | CPU s/100k base | Canonical | Bytes/row base | Canonical |
|---|---|---|---|---|---|---|---|
| MySQL CDC JSON narrow | 82,488 | 61,828 | 0.75 (0.68-1.00) | 1.62 | 2.20 | 841 | 972 |
| MySQL CDC JSON wide | 21,171 | 15,901 | 0.75 (0.73-0.83) | 6.06 | 7.68 | 2,105 | 2,236 |
| MySQL CDC JSON binary | 34,224 | 32,013 | 0.94 (0.85-0.99) | 3.76 | 3.88 | 2,217 | 2,334 |
| MySQL CDC JSON temporal | 45,802 | 28,527 | 0.62 (0.57-0.65) | 3.32 | 4.39 | 1,259 | 1,449 |
| MySQL CDC JSON null-heavy | 34,275 | 26,459 | 0.77 (0.76-0.82) | 3.78 | 4.66 | 1,491 | 1,615 |
| MySQL CDC Avro narrow | 31,705 | 28,284 | 0.89 (0.86-0.92) | 3.43 | 4.01 | 454 | 551 |
| MySQL CDC Avro wide | 9,753 | 7,617 | 0.78 (0.72-0.80) | 11.74 | 14.72 | 1,144 | 1,237 |
| MySQL CDC Parquet wide | 11,326 | 8,571 | 0.76 (0.71-0.82) | 7.12 | 9.86 | 677 | 677 |
| MySQL snapshot JSON narrow | 85,837 | 65,204 | 0.76 (0.75-0.79) | 1.90 | 2.11 | 670 | 786 |
| MySQL snapshot JSON temporal | 46,958 | 28,881 | 0.62 (0.54-0.64) | 3.64 | 4.47 | 1,137 | 1,263 |
| MySQL CDC JSON ints10 | 60,493 | 50,389 | 0.83 (0.72-0.89) | 1.94 | 2.42 | 916 | 1,033 |
| MySQL CDC JSON ints200 | 9,870 | 7,870 | 0.80 (0.75-0.81) | 13.04 | 15.88 | 4,132 | 4,251 |
| MySQL CDC JSON narrow + JS | 19,588 | 15,381 | 0.79 (0.76-0.81) | 5.78 | 7.40 | 866 | 1,000 |
| PostgreSQL snapshot JSON narrow | 78,393 | 60,506 | 0.77 (0.75-0.80) | 1.81 | 2.26 | 718 | 787 |

Database CPU was the same for both versions (within 0.1 s per cell). Peak RSS was the same or lower for the canonical version.

### 2.2 First full pass (one repetition, `target/perf/full1`)

The same picture across the rest of the matrix: MySQL JSON, Avro and Parquet cells at 0.73-0.93, temporal cells 0.56-0.59, binary and Parquet-narrow cells 0.89-1.05, and latency cells unchanged (1.00). PostgreSQL snapshot cells match MySQL's (0.59-0.94).

**PostgreSQL CDC is not a canonical comparison in this pass.** Both versions ran PostgreSQL CDC at 7-9k rows/s, capped by a pre-existing per-event state-store read in WAL feedback (37% of CPU in SQLite, serial in the source loop). That was fixed separately (#154: about 10x on a SQLite state store, 68x on a PostgreSQL one), so these cells measured the bottleneck, not encoding (ratios 0.85-1.36).

### 2.3 Cells only the canonical version runs

The base version could not produce correct output for these cells. These are the pre-merge defects #153 fixed: Avro and Parquet serialization of MySQL snapshots and temporal values, PostgreSQL CDC to Avro, Parquet `timestamptz`, and JavaScript on wide rows. Its markers or rows failed, so there is no ratio; the canonical version's achieved performance (one repetition) is:

| Cell | Canonical rows/s | CPU s/100k |
|---|---|---|
| MySQL CDC Avro temporal | 14,956 | 7.51 |
| MySQL CDC Parquet temporal | 21,164 | 3.20 |
| MySQL CDC JSON wide + JS | 4,684 | 24.20 |
| MySQL snapshot Avro narrow / wide / binary / null-heavy / temporal | 26,557 / 7,784 / 20,549 / 9,394 / 15,118 | 4.16 / 14.80 / 5.60 / 12.10 / 7.50 |
| MySQL snapshot Parquet narrow / wide / null-heavy / temporal | 19,811 / 7,764 / 7,461 / 17,124 | 1.28 / 9.80 / 7.24 / 3.45 |
| PostgreSQL CDC Avro narrow / wide / binary / null-heavy / temporal | 4,936 / 3,384 / 4,937 / 6,015 / 6,442 | 28.85 / 47.74 / 27.44 / 30.92 / 26.32 |
| PostgreSQL CDC Parquet narrow / temporal | 6,747 / 7,354 | 18.60 / 18.61 |

The PostgreSQL CDC rows predate #154 and are capped by the same per-event read.

## 3. Diagnosis

### 3.1 CPU

Profiles (`perf record -g`, frame pointers, profiling builds of both revisions, same cells), share of DeltaForge CPU per stage:

| Stage | narrow JSON base | canonical | temporal JSON base | canonical | wide Avro base | canonical |
|---|---|---|---|---|---|---|
| MySQL decode (incl. canonical value construction) | 4.7% | 7.8% | 20.5% | 10.6% | 7.0% | 9.6% |
| JSON rendering (canonical row to `serde_json::Value`) | 0 | 11.3% | 0 | 35.0% | 0 | 15.1% |
| JSON serialization | 16.6% | 12.6% | 12.8% | 7.2% | 1.1% | 0.8% |
| Avro conversion | - | - | - | - | 67.5% | 61.7% |
| Kafka producer + LZ4 | 26.6% | 26.5% | 29.6% | 24.0% | 8.5% | 7.5% |
| Relation identity / idempotency key | 3.5% | 7.4% | 2.2% | 3.2% | 0.3% | 1.2% |
| Allocator (self) | 17.6% | 24.4% | 17.0% | 19.5% | 19.2% | 19.0% |

Every regression above 10% is explained by four costs, all on the delivery path:

1. **Per-row JSON map construction.** `render_row` builds a `serde_json::Map<String, Value>` per row: an owned column-name string and a `Value` per column. serde_json's `preserve_order` (enabled through deno_core) makes it an IndexMap, so every column name is also hashed. This is the 11-15% rendering share in narrow and wide cells, most of the allocator growth, and why cost grows with column count (ints10 to ints200: per-column cost up about 22-25%, linear, no nonlinear width behavior).
2. **Temporal text through `format!`.** Each temporal value is rendered by several `format!` calls (year, clock, fraction), each a heap string; `format_inner`, `fmt::write` and integer formatting dominate the temporal cells (35% rendering). The base version produced the text while decoding (its decode share was 20.5%); canonical decoding is cheaper (10.6%), but rendering is far more expensive.
3. **Avro through JSON.** Avro encoding renders the whole native envelope to a `serde_json::Value` and then converts row values from canonical values: the row is rendered twice (15.1% rendering in the wide Avro cell). Parquet wide (0.76) is consistent with the same envelope rendering; it was not profiled separately.
4. **Relation identity per event.** The default Kafka key now embeds `SourceRelationIdentity::canonical()`, rebuilt (allocated and escaped) for every event; the bounded S3 key segment adds a SHA-256 for long names (visible as `sha2::digest_blocks`).

Cloning and dropping events or rows on the delivery path measured under 1% in every profile. Per-value schema dispatch, repeated canonicalization of values and the durable stored form were not attributed separately; none appeared among the top self-time functions, so each is below the costs above but unmeasured. Reallocation of growing buffers (`rallocx`, `arena_ralloc`, 2-3%) is present in both versions.

Binary rows are nearly unaffected (0.94): bytes are carried, not reformatted. Latency cells are unchanged: at 200 rows/s the per-row cost is invisible.

### 3.2 Output size

JSON and Avro output grows by a roughly constant 100-130 bytes per row, whatever the row width:

- **Kafka key:** it now embeds the relation identity `connector/source/database/namespace/table` instead of `database.table`. It is longer by the connector, the source name and the namespace, about 40-90 bytes with these pipeline names.
- **`source.type_mapping`** (`"type_mapping":"v2"`): about 20 bytes.
- **Temporal cells:** up to about 60 bytes more per row than the narrow cells (CDC about 60, snapshot about 10), from the canonical text form of temporal values.
- **Harness artifact:** pipeline names carry the version label, and `canonical` is 5 bytes longer than `base`, so `source.name` and the key are a few bytes longer for the canonical runs. This is not a product cost.

Parquet output is unchanged (677 bytes per row). Output size adds broker and network cost but did not drive the CPU differences above; Kafka plus LZ4 kept the same CPU share.

## 4. Optimizations

The four costs of section 3, and the further costs found by profiling the optimized build, are removed by separate commits. None changes any output. Native JSON is checked byte for byte against a golden written by the previous renderer: every value kind, every temporal precision, both mapping versions and both wide-integer modes. Avro is checked against a golden written by the previous encoder: every MySQL column type, both schema paths, three envelopes, inserts, updates, deletes and extensions. The key writers are checked against the formulas they replace, and the script's input object against the one built through a JSON tree. Twenty-five targeted mutants were run against these tests and the coordinator's and Kafka sink's suites; twenty-four are caught. One behavior has no test that reaches it: an Avro batch is still prepared in full before its first record is enqueued, and only a schema registry failure part-way through a batch would show the difference. The survivor writes an Avro row placeholder as `null` instead of `{}`, which is equivalent for every table with at least one column.

| Commit | Change |
|---|---|
| Streaming JSON | A row image is checked once, then written to the serializer column by column: no `Map`, no owned names, no per-column hashing. |
| Temporal text | Dates, times, date-times and timestamps are written digit by digit into a stack buffer: no `format!`, no intermediate strings. |
| Direct Avro | The envelope JSON holds `{}` where the rows go: Path A converts them from canonical values, and Path C rebuilds them. Each registered schema is prepared once (named types resolved, a writer that validates and encodes with them) instead of being resolved three times per event. Field paths and source types are written out only for a failure. |
| Identity and key material | The default Kafka key writes the relation's canonical encoding and the event id into one buffer. The Avro Path A cache is keyed by one length-prefixed string, so a cached event builds no relation, subject or schema identity. |
| Constant-time column lookup | Parquet, Avro and ClickHouse fetched each schema column from the row by a linear scan, so a row's cost grew with the square of its width; the Parquet column check also formatted a name and scanned the schema per column. Lookups now try the column's schema position first. |
| Row construction | A decoded row's duplicate-name check no longer allocates a hash set (up to 32 columns). |
| Kafka headers and payload | A batch built each message's headers and then enqueued a clone: two sets of librdkafka allocations per message, the original freed on the worker thread. Records now take their headers. Native JSON is serialized straight into the message buffer, which starts at the size of the thread's previous payload instead of growing from 128 bytes. A static topic is borrowed instead of cloned. |
| Event id text | The id's text (in every payload, key and script input) was built by hex-encoding into a `String` and formatting it. It is now written from a stack buffer. |
| JS inputs | Each script input was rendered to a JSON tree (its payload, then clones of its JavaScript-safe rows) and that tree converted to V8. It is now serialized into V8 from the event and the renderings kept for re-typing, with the same keys in the same order. |
| Schema references | Batch schema resolution and the JS processor built an owned reference (five strings) and hashed it for every event. Consecutive events of one version are now compared with it in place, and column types are looked up at their schema position first. |
| Kafka enqueue as prepared | A batch was encoded in full before its first record reached librdkafka, so encoding time and the wait for delivery reports added up (about 7 ms and 15-25 ms per 2,000-event batch). Native JSON records are now enqueued as soon as each is prepared, so librdkafka sends early records while later ones are encoded. A JSON record fails only per event, to the DLQ. Avro batches are still prepared in full first: a schema registry failure fails the whole batch, and then nothing of it may be enqueued. A transactional batch begins its transaction before its first record. |
| Processing overlapped with delivery | The coordinator's delivery task ran a batch's processors, then delivered and committed it, so processing and delivery times added up. Once a batch is processed, the next queued item's processors now run while it is delivered. Processing stays sequential and in order; delivery and commits stay one at a time and in order; a processing failure surfaces only after the batches before it are delivered and committed. At most one more batch is held (processed, awaiting delivery). |

### 4.1 Per hotspot, in process

`cargo bench -p deltaforge-core --bench canonical_encode` runs one event at a time through the production path:
- JSON: `Native.wrap` then `Json.encode`;
- Avro: `AvroEncoder::encode_event` with DDL-derived schemas and a local registry;
- the key: `Event::idempotency_key`.

It reports the median of 9 runs and the exact allocations per event, from a counting allocator. `cargo bench -p processors --bench js_boundary_bench` measures the coordinator's batch schema resolution and the JS processor, with the perf harness's script on narrow rows.

| Case | main (ns, allocs) | optimized |
|---|---|---|
| JSON narrow | 1,231, 25 | 655, 5 |
| JSON wide (60 columns) | 12,301, 219 | 2,912, 7 |
| JSON temporal (20 columns) | 7,865, 178 | 1,857, 5 |
| JSON null-heavy | 6,512, 93 | 1,742, 6 |
| JSON 200 integers | 26,699, 242 | 4,614, 7 |
| Avro narrow | 18,077, 544 | 8,813, 128 |
| Avro wide | 84,798, 2,234 | 23,961, 268 |
| Avro temporal | 37,552, 1,153 | 12,689, 165 |
| Kafka key | 464, 12 | 79, 1 |
| JS processor (per event) | 20,085-22,586, 172 | 17,256, 111 |
| Batch schema resolution (per event) | 178-244, 5 | 125, 0 |

For comparison, the same benchmarks built from the base version (`d44d0be`) give:

| Case | Base version |
|---|---|
| JSON narrow | 673 ns, 9 allocations |
| Envelope alone | 517 ns |
| Kafka key | 189 ns |
| JS processor | 18,318 ns, 138 allocations |

JSON cost no longer grows with column count beyond the values themselves: 5 to 7 allocations per event at any width.

**Where the JavaScript time goes.** Instrumenting the JS processor at the PR's first version split its 20 µs per event into:

| Phase | ns per event |
|---|---|
| Rendering the inputs (`to_js`) | 3,975 |
| Into V8 | 4,436 |
| V8 executing the script | 652 |
| Back from V8 | 7,553 |
| Lineage and re-typing | 2,906 |

The script itself is 3% of the cost; the boundary is the rest. Writing inputs straight into V8 removed most of the rendering phase. Reading outputs back stays with `serde_v8` into JSON values. JSON text transport (`JSON.stringify` and a Rust parser) was rejected because it changes what a script's output means in real cases:
- an `undefined` value disappears instead of reading as null;
- dates become strings;
- functions vanish;
- a large integer such as `3000000000` arrives as an integer where `serde_v8` gives a float, which changes the rendered bytes of an extension.

A typed reader saved too little, because exact semantics still require enumerating every property.

### 4.2 Interleaved matrix

The cells of section 2.1 and the two latency cells, release builds, 5 repetitions alternating version order (`target/perf/final3`). Base is `d44d0be`; optimized is `bf0ebe77`, whose runtime code is that of the final commit. CPU and system time are seconds per 100k rows. The paired ratios are repetition *i* of the optimized build over repetition *i* of the base, run next to each other, so each pair shares the host's state.

| Cell | Base rows/s | Optimized | Median ratio | Paired min-max | Pairs at or above 1.00 | CPU base | Optimized | System base | Optimized | Bytes/row base | Optimized |
|---|---|---|---|---|---|---|---|---|---|---|---|
| MySQL CDC JSON narrow | 113,483 | 135,352 | 1.19 | 1.16-1.21 | 5/5 | 1.01 | 0.95 | 0.12 | 0.12 | 844 | 945 |
| MySQL CDC JSON wide | 36,736 | 46,005 | 1.25 | 1.21-1.27 | 5/5 | 3.38 | 3.12 | 0.66 | 0.52 | 2,108 | 2,208 |
| MySQL CDC JSON binary | 58,908 | 73,379 | 1.25 | 1.18-1.27 | 5/5 | 2.04 | 1.94 | 0.60 | 0.52 | 2,220 | 2,307 |
| MySQL CDC JSON temporal | 78,869 | 91,576 | 1.16 | 1.10-1.22 | 5/5 | 1.77 | 1.61 | 0.31 | 0.26 | 1,263 | 1,421 |
| MySQL CDC JSON null-heavy | 58,289 | 71,898 | 1.23 | 1.17-1.27 | 5/5 | 2.14 | 1.80 | 0.48 | 0.36 | 1,494 | 1,588 |
| MySQL CDC Avro narrow | 50,597 | 70,653 | 1.40 | 1.34-1.45 | 5/5 | 1.97 | 1.50 | 0.09 | 0.11 | 456 | 523 |
| MySQL CDC Avro wide | 16,670 | 28,009 | 1.68 | 1.56-1.70 | 5/5 | 6.68 | 4.06 | 0.58 | 0.48 | 1,146 | 1,209 |
| MySQL CDC Parquet wide | 19,910 | 22,102 | 1.11 | 0.99-1.12 | 4/5 | 3.72 | 3.10 | 0.60 | 0.46 | 677 | 677 |
| MySQL snapshot JSON narrow | 122,581 | 225,824 | 1.84 | 1.68-1.85 | 5/5 | 1.07 | 0.90 | 0.15 | 0.22 | 670 | 756 |
| MySQL snapshot JSON temporal | 83,296 | 119,892 | 1.44 | 1.38-1.46 | 5/5 | 1.91 | 1.66 | 0.36 | 0.37 | 1,137 | 1,233 |
| MySQL CDC JSON ints10 | 87,199 | 104,749 | 1.20 | 1.14-1.29 | 5/5 | 1.28 | 1.18 | 0.24 | 0.22 | 919 | 1,006 |
| MySQL CDC JSON ints200 | 18,948 | 21,299 | 1.12 | 1.08-1.15 | 5/5 | 6.60 | 6.62 | 1.48 | 1.28 | 4,135 | 4,224 |
| MySQL CDC JSON narrow + JS | 34,335 | 38,948 | 1.13 | 1.07-1.17 | 5/5 | 3.14 | 3.26 | 0.38 | 0.36 | 869 | 973 |
| PostgreSQL snapshot JSON narrow | 113,440 | 176,473 | 1.56 | 1.41-1.66 | 5/5 | 1.01 | 0.93 | 0.16 | 0.22 | 718 | 763 |

**Variability.** Every cell's median is at least 1.11. In thirteen of the fourteen cells every one of the five pairs is above 1.00, and the lowest of them is 1.07 (JavaScript). Parquet wide has one pair at 0.99 against a median of 1.11; its other four pairs are 1.10 to 1.12. The spread of the paired ratios within a cell is 0.03 to 0.15, so no cell's median is within its own noise of 1.00.

**Latency** (the latency cells run 2,000 single-row transactions at a fixed 200 per second, so their rate is not a measure):

| Cell | p50/p99 ms base | Optimized |
|---|---|---|
| MySQL latency JSON narrow | 56/107 | 56/107 |
| MySQL latency Avro narrow | 58/107 | 57/107 |

Latency is unchanged.

**Where the improvement comes from.** In-process costs are at or below the base version's (section 4.1). The two scheduling changes remove waits the base version also had:
- Kafka records are enqueued as they are encoded.
- A pipeline's processors run while the previous batch is delivered.

They do not change what is delivered, in what order, or when a checkpoint commits. The optimized build sends 100-160 more bytes per row (the relation identity in the key, `source.type_mapping`, temporal text; section 3.2) and still delivers more rows per second.

Against the current main (`6b568a1`; `target/perf/opt-vs-main`, measured on the first optimized commit), the optimized build is as fast or faster in every cell, up to 2.12 times:
- MySQL CDC JSON: 1.00 (ints10) to 1.38;
- Avro: 1.43 to 2.06;
- Parquet: 1.47 to 1.53;
- snapshots: 1.18 to 2.12;
- JavaScript: 1.05 to 1.15;
- PostgreSQL CDC, no longer capped (see section 2.2): 1.16 for JSON, 1.51 for Avro and 1.53 for Parquet.

The scheduling changes came after that run, so the current figures are higher.

**Allocation requests per row** (stats builds, one repetition; `target/perf/final3-allocs`, and `final-allocs2-main` for main):

| Cell | Base | main | Optimized |
|---|---|---|---|
| MySQL CDC JSON narrow | 57 | 88 | 46 |
| MySQL CDC JSON wide | 252 | 408 | 185 |
| MySQL CDC JSON binary | 97 | 117 | 68 |
| MySQL CDC JSON temporal | 118 | 236 | 55 |
| MySQL CDC JSON null-heavy | 120 | 208 | 103 |
| MySQL CDC Avro narrow | 570 | 651 | 183 |
| MySQL CDC Avro wide | 2,186 | 2,450 | 462 |
| MySQL CDC Parquet wide | 382 | 513 | 321 |
| MySQL snapshot JSON narrow | 62 | 78 | 37 |
| MySQL snapshot JSON temporal | 114 | 230 | 49 |
| MySQL CDC JSON ints10 | 55 | 82 | 43 |
| MySQL CDC JSON ints200 | 266 | 487 | 254 |
| MySQL CDC JSON narrow + JS | 209 | 325 | 171 |
| PostgreSQL snapshot JSON narrow | 48 | 78 | 37 |

Every cell allocates less than the base version. Temporal and wide rows carry no allocation penalty, and nothing grows faster than linearly with row width.

### 4.3 System time and the C allocator

In the narrow Kafka cells the optimized build's user time per row is that of the base version (0.90 against 0.89 seconds per 100k rows). Its extra CPU is system time: 0.18-0.20 against 0.12. The harness's kernel counters show the same read and write system calls per row, but almost twice the minor page faults: 0.82 per row against 0.45.

Two experiments identify the source:
- Raising glibc's trim and mmap thresholds, so freed heap memory is not returned to the kernel, takes the optimized build to 0.27-0.31 faults per row (below the base version) and to 0.07-0.09 seconds of system time.
- Disabling jemalloc's page purging changes nothing.

The faults therefore come from glibc's heap, which only the C libraries use (librdkafka here). librdkafka allocates the per-message `df-type-mapping` header (the mapping version, part of the canonical contract) as three small blocks on the producing thread and frees them on its poll thread. That churn makes glibc consolidate and trim its heaps, and the trimmed pages fault back in.

The code fixes in this section halved that churn (the header clone is gone). With records enqueued as they are encoded, the narrow cell's system time is the base version's (0.12 against 0.12 seconds per 100k rows), and every cell is faster than the base version without changing any allocator. Tuning glibc for C libraries (trim and mmap thresholds, by `mallopt` at startup or by environment) would remove the rest. Linking jemalloc unprefixed would do the same. Either changes how every bundled C library allocates and how much memory it keeps, so it is left for a separate, qualified decision. That decision should cover memory use, latency, durability and long-running workloads.
## 5. Kafka key audit

The default Kafka key is `tenant|relation|transaction|event id` (`Event::idempotency_key`), where the relation is the canonical `connector/source/database/namespace/table` (the base version used `database.table`). The key is longer by the connector, the source name and the namespace: about 40-90 bytes with typical names.

- **The event id** (`dfid:v1:<class>:<32 hex>`) is a hash of the event's source coordinates. For MySQL without GTIDs these are the server id, binlog file, position and row ordinal, which do not name the source: two sources reading servers with the same `server_id` (often the default 1) can produce the same id.
- **The source name** is what separates such sources. It must stay. The connector separates nothing the event id's class does not, and the database, namespace and table are implied by the position. They are redundant for uniqueness, but they make the key readable and match every other relation-keyed store.
- **The transaction id** is redundant given the event id.
- **The tenant** keeps the keys of different tenants apart.

A fixed digest of these parts (32 hex characters of SHA-256) would save 60-90 bytes per message with these names. It would weaken nothing in uniqueness. But it changes every key: a consumer that deduplicates by key would see a redelivered event under a new key after an upgrade, and it makes keys unreadable. Partitioning is unaffected either way, since the default key is unique per event and spreads events across partitions; that is unchanged from the base version. Recommendation: keep the format. A shorter key is a separate, versioned key-format decision, which this work does not make.
