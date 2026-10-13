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
- allocations: not available. The release jemalloc is built without statistics (`config.stats` false), so allocation pressure is read from CPU profiles instead (section 3);
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

## 4. Candidate optimizations (deferred)

Absolute throughput is low in both versions, and optimization is deferred by decision. These candidates keep the reviewer's constraints: exact values, the mapping version kept, no unsafe wide integers, encoding plans cached by schema reference and mapping version, no per-row schema or name ownership, and output size reported separately from CPU.

1. Serialize canonical rows directly into a reusable, pre-sized output buffer, with column names written from the cached schema, not owned per row (no `Map`, no `Value`). This addresses cost 1, most of the allocator growth, and part of cost 3.
2. Render temporal values from their typed components with a fixed-width writer (no `format!`), parsing once at decode (cost 2).
3. Encode Avro from canonical values with a plan cached per schema reference and mapping version, without the JSON envelope (cost 3).
4. Cache each relation's identity string, key prefix and segment digest per relation and mapping version (cost 4).
5. Pre-size per-batch buffers from the previous batch's size.

Each needs the interleaved rerun of section 2.1 afterwards, with output size reported separately from CPU.
