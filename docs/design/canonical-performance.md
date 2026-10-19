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

The four costs of section 3, and three more found by profiling the optimized build, are removed by separate commits. None changes any output: native JSON is checked byte for byte against a golden written by the previous renderer (every value kind, every temporal precision, both mapping versions and both wide-integer modes), Avro against a golden written by the previous encoder (every MySQL column type, both schema paths, three envelopes, inserts, updates, deletes and extensions), and the key writers against the formulas they replace. Thirteen targeted mutants were run against these tests; twelve are caught, and the survivor (writing an Avro row placeholder as `null` instead of `{}`) is equivalent for every table with at least one column.

| Commit | Change |
|---|---|
| 1. Streaming JSON | A row image is checked once, then written to the serializer column by column: no `Map`, no owned names, no per-column hashing. |
| 2. Temporal text | Dates, times, date-times and timestamps are written digit by digit into a stack buffer: no `format!`, no intermediate strings. |
| 3. Direct Avro | The envelope JSON holds `{}` where the rows go (Path A converts them from canonical values, Path C rebuilds them), so no row is rendered to be discarded. Each registered schema is prepared once (named types resolved, a writer that validates and encodes with them) instead of resolving the schema three times per event. Field paths and source types are written out only for a failure. |
| 4. Identity and key material | The default Kafka key writes the relation's canonical encoding and the event id into one buffer. The Avro Path A cache is keyed by one length-prefixed string, so a cached event builds no relation, subject or schema identity. |
| 5. Constant-time column lookup | Parquet, Avro and ClickHouse fetched each schema column from the row by a linear scan, so a row cost grew with the square of its width; the Parquet column check also formatted a name and scanned the schema per column. Lookups now try the column's schema position first. |
| 6. JS boundary | Each row image was rendered three times and cloned twice per event; now once and once. |
| 7. Row construction | A decoded row's duplicate-name check no longer allocates a hash set (up to 32 columns). |

### 4.1 Per hotspot, in process

`cargo bench -p deltaforge-core --bench canonical_encode`: one event at a time through the production path (`Native.wrap` then `Json.encode`; `AvroEncoder::encode_event` with DDL-derived schemas and a local registry; `Event::idempotency_key`), median of 9 runs, exact allocations per event from a counting allocator.

| Case | main (ns, allocs) | after 1 | after 2 | after 3 | after 4 | final |
|---|---|---|---|---|---|---|
| JSON narrow | 1,231, 25 | 1,117, 18 | 848, 9 | | | 888, 9 |
| JSON wide (60 columns) | 12,301, 219 | 6,120, 123 | 3,169, 11 | | | 3,368, 11 |
| JSON temporal (20 columns) | 7,865, 178 | 6,383, 154 | 2,019, 9 | | | 2,033, 9 |
| JSON null-heavy | 6,512, 93 | 2,344, 21 | 1,928, 10 | | | 1,870, 10 |
| JSON 200 integers | 26,699, 242 | 4,644, 11 | 4,733, 11 | | | 4,751, 11 |
| Avro narrow | 18,077, 544 | 16,812, 537 | 16,836, 528 | 8,019, 141 | 7,605, 123 | 7,478, 123 |
| Avro wide | 84,798, 2,234 | 74,081, 2,138 | 65,741, 1,946 | 33,009, 281 | 31,636, 263 | 26,149, 263 |
| Avro temporal | 37,552, 1,153 | 35,491, 1,129 | 29,293, 934 | 12,890, 178 | 12,554, 160 | 12,373, 160 |
| Kafka key | 464, 12 | | | | 85, 1 | 73, 1 |

Within commit 3, the row placeholders took Avro wide to 56,398 ns (1,834 allocations), prepared schemas to 35,211 (487) and deferred paths to 33,009 (281). JSON cost no longer grows with column count beyond the values themselves: 9 to 11 allocations per event at any width.

### 4.2 Interleaved matrix

The cells of section 2.1, release builds, 3 repetitions alternating version order (`target/perf/final-accept`; base `d44d0be`, optimized `ab85d33f`). The host was less loaded than for section 2.1, so absolute rates are higher for both versions.

| Cell | Base rows/s | Optimized | Ratio | CPU s/100k base | Optimized | Bytes/row base | Optimized |
|---|---|---|---|---|---|---|---|
| MySQL CDC JSON narrow | 116,469 | 101,576 | 0.87 | 0.99 | 1.18 | 841 | 940 |
| MySQL CDC JSON wide | 36,820 | 37,245 | 1.01 | 3.38 | 3.22 | 2,105 | 2,205 |
| MySQL CDC JSON binary | 53,678 | 60,472 | 1.13 | 2.22 | 1.92 | 2,217 | 2,303 |
| MySQL CDC JSON temporal | 78,242 | 71,749 | 0.92 | 1.76 | 1.76 | 1,259 | 1,418 |
| MySQL CDC JSON null-heavy | 58,056 | 60,484 | 1.04 | 2.10 | 2.00 | 1,491 | 1,586 |
| MySQL CDC Avro narrow | 50,596 | 66,699 | 1.32 | 1.98 | 1.56 | 454 | 521 |
| MySQL CDC Avro wide | 17,127 | 27,646 | 1.61 | 6.52 | 4.14 | 1,144 | 1,207 |
| MySQL CDC Parquet wide | 19,925 | 22,366 | 1.12 | 3.80 | 3.06 | 677 | 677 |
| MySQL snapshot JSON narrow | 125,858 | 166,441 | 1.32 | 1.04 | 1.04 | 670 | 756 |
| MySQL snapshot JSON temporal | 85,149 | 87,949 | 1.03 | 1.89 | 1.82 | 1,137 | 1,233 |
| MySQL CDC JSON ints10 | 88,873 | 82,471 | 0.93 | 1.22 | 1.38 | 916 | 1,003 |
| MySQL CDC JSON ints200 | 19,106 | 18,118 | 0.95 | 6.66 | 6.90 | 4,132 | 4,221 |
| MySQL CDC JSON narrow + JS | 34,112 | 29,521 | 0.87 | 3.12 | 3.64 | 866 | 970 |
| PostgreSQL snapshot JSON narrow | 107,964 | 142,692 | 1.32 | 1.07 | 1.19 | 718 | 763 |

Against the current main (`6b568a1`, the canonical model with every later change; `target/perf/opt-vs-main`, measured before commit 7), the optimized build is as fast or faster in every cell, up to 2.12 times: MySQL CDC JSON 1.00 (ints10) to 1.38, Avro 1.43 to 2.06, Parquet 1.47 to 1.53, snapshots 1.18 to 2.12, JavaScript 1.05 to 1.15, and PostgreSQL CDC (no longer capped, see section 2.2) 1.16 (JSON), 1.51 (Avro) and 1.53 (Parquet).

**Allocation requests per row** (stats builds, one repetition; `target/perf/final-allocs2-base`, `final-allocs2-main`):

| Cell | Base | main | Optimized |
|---|---|---|---|
| MySQL CDC JSON narrow | 57 | 88 | 56 |
| MySQL CDC JSON wide | 252 | 408 | 196 |
| MySQL CDC JSON temporal | 118 | 236 | 66 |
| MySQL CDC Avro narrow | 570 | 651 | 188 |
| MySQL CDC Avro wide | 2,186 | 2,450 | 468 |
| MySQL CDC Parquet wide | 382 | 513 | 324 |
| MySQL snapshot JSON temporal | 114 | 230 | 60 |
| MySQL CDC JSON ints200 | 266 | 487 | 265 |
| MySQL CDC JSON narrow + JS | 209 | 325 | 254 |

Every cell allocates no more than the base version except JavaScript. The temporal and wide cells have no allocation penalty left, and nothing grows faster than linearly with row width.

### 4.3 What remains below 0.90

**MySQL CDC JSON narrow (0.87 to 0.89 over three runs) and narrow + JS (0.86 to 0.87).** Their CPU per row is 15-17% above the base version. Profiles of the two versions (same cell, 2 repetitions each) put the difference in librdkafka, not in DeltaForge's code: the optimized build spends about 375 samples in glibc's allocator (`_int_free`, `malloc_consolidate`, `malloc`) against about 40 for the base version, out of a total difference of 279, on the producer's poll thread freeing messages. The canonical model added the `df-type-mapping` header to every Kafka message (the mapping version a consumer needs to read the values); librdkafka allocates each message's headers with glibc and frees them on another thread. Output is also about 100 bytes per row larger (the relation identity in the key, `source.type_mapping`; section 3.2), which LZ4 and the broker carry.

Linking jemalloc unprefixed (`tikv-jemallocator/unprefixed_malloc_on_supported_platforms`, so C libraries allocate through jemalloc too) recovers it, measured with 3 repetitions: CDC JSON narrow 0.95, temporal 0.96, ints10 0.91, Avro wide 1.64, with narrow CPU at parity (1.04 against 1.03). It is not part of these commits, because it changes operations: every bundled C library (librdkafka, SQLite, zlib) would allocate through jemalloc, and jemalloc's environment variable becomes `MALLOC_CONF` instead of `_RJEM_MALLOC_CONF`, so existing tuning (the chaos compose file sets it) would silently stop applying. It needs a reviewed decision.

**JavaScript.** With unprefixed jemalloc the JS cell stays at 0.85. Its remaining cost is spread over V8, `serde_v8` and the typed boundary, which re-types every script output against the input it came from (section 3.1 of the canonical model). The boundary allocates 254 times per row against 209 for the base version, which passed untyped JSON. No single cost dominates; the typed re-typing is part of the correctness contract.

## 5. Kafka key audit

The default Kafka key is `tenant|relation|transaction|event id` (`Event::idempotency_key`), where the relation is the canonical `connector/source/database/namespace/table` (the base version used `database.table`). The key is longer by the connector, the source name and the namespace: about 40-90 bytes with typical names.

- **The event id** (`dfid:v1:<class>:<32 hex>`) is a hash of the event's source coordinates. For MySQL without GTIDs these are the server id, binlog file, position and row ordinal, which do not name the source: two sources reading servers with the same `server_id` (often the default 1) can produce the same id.
- **The source name** is what separates such sources. It must stay. The connector separates nothing the event id's class does not, and the database, namespace and table are implied by the position. They are redundant for uniqueness, but they make the key readable and match every other relation-keyed store.
- **The transaction id** is redundant given the event id.
- **The tenant** keeps the keys of different tenants apart.

A fixed digest of these parts (32 hex characters of SHA-256) would save 60-90 bytes per message with these names. It would weaken nothing in uniqueness. But it changes every key: a consumer that deduplicates by key would see a redelivered event under a new key after an upgrade, and it makes keys unreadable. Partitioning is unaffected either way, since the default key is unique per event and spreads events across partitions; that is unchanged from the base version. Recommendation: keep the format. A shorter key is a separate, versioned key-format decision, which this work does not make.
