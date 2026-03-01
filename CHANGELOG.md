# Changelog

All notable changes to DeltaForge will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Synthetic event support** - Framework-level detection and marking of processor-generated events ([c0b229c](https://github.com/vnvo/deltaforge/commit/c0b229c815b4068e1b8a97c97c6c88c3162df9ea))
  - `BatchContext` captures original event IDs before the processor chain runs
  - `SyntheticMarkingProcessor` wrapper automatically marks any event with a new ID as synthetic, setting `event.synthetic = Some(processor_id)` — no processor code changes required
  - Processors that explicitly set `synthetic` are respected (no overwrite)
  - `Processor` trait signature updated to `process(&self, events, ctx: &BatchContext)`
  - `Event::is_synthetic()` convenience method
  - `build_processors()` wraps every processor with `SyntheticMarkingProcessor` transparently
- **Sink event filtering** - Per-sink filtering to route real vs. synthetic events to different destinations ([784223d](https://github.com/vnvo/deltaforge/commit/784223de64114fffc0439f820119cbe1deadc1a2))
  - `filter.exclude_synthetic: true` — drop synthetic events (e.g. send only real CDC events to Kafka)
  - `filter.synthetic_only: true` — drop real events (e.g. send only metric events to a dedicated stream)
  - Available on all sink types: Kafka, Redis, NATS
  - Zero overhead when no filter is configured
- **Flatten processor** - Native Rust processor that flattens nested JSON objects in event payloads into top-level keys joined by a configurable separator ([0f04b49](https://github.com/vnvo/deltaforge/commit/0f04b49e29df261756546d585b50ec560d1e49be))
  - Works on any object-valued field present on the event (before, after, or custom fields from upstream processors) - no CDC structure assumptions
  - `separator`: key path joiner (default: `"__"`)
  - `max_depth`: stop recursing at this depth; objects at boundary kept as opaque leaves
  - `on_collision`: `last` (default), `first`, or `error` when two paths produce the same key
  - `empty_object`: `preserve` (default), `drop`, or `"null"`
  - `lists`: `preserve` (default) or `index` (expand arrays to `field__0`, `field__1`, …)
  - `empty_list`: `preserve` (default), `drop`, or `"null"`
  - Envelope interaction: runs before sink serialization - all envelope formats (native, debezium, cloudevents) wrap already-flattened data
  - Compatible with outbox + `raw_payload: true` - flatten runs on the extracted payload before raw delivery

- **Outbox pattern support** - Zero-footprint transactional outbox for MySQL and PostgreSQL ([9df087d](https://github.com/vnvo/deltaforge/commit/9df087d196cd3a5c199bb58d37e643915b0a5324))
  - PostgreSQL: pg_logical_emit_message() captured via ReplicationEvent::Message ([346272c](https://github.com/vnvo/deltaforge/commit/346272c3b8e4be01745b59a6ec413c9b19d6243f))
  - MySQL: BLACKHOLE engine tables captured from binlog ([0ede50c](https://github.com/vnvo/deltaforge/commit/0ede50c24406916a87407cea92944f435a7b84aa))
  - Source-level outbox config with AllowList glob patterns for multi-table/prefix matching
  - Explicit OutboxProcessor with ${...} template-based topic routing
  - Per-channel routing via tables filter on processor (multi-outbox support)
  - Topic resolution cascade: template -> column -> default_topic
  - Additional headers and raw payload delivery mode ([3bb998e](https://github.com/vnvo/deltaforge/commit/3bb998e4b24b0d4e46c44d5e5ad98e6e85aabd4f))
  - Key routing with template support (`key: "${aggregate_id}"`), defaults to aggregate_id ([84d621d](https://github.com/vnvo/deltaforge/commit/84d621d27ddc303a0ef24bf2ffc03619335643bf))
  - Event ID extraction (`df-event-id` header) from configurable column (default: `id`) ([84d621d](https://github.com/vnvo/deltaforge/commit/84d621d27ddc303a0ef24bf2ffc03619335643bf))
  - Typed extraction for headers — strings, numbers, booleans stringified automatically ([84d621d](https://github.com/vnvo/deltaforge/commit/84d621d27ddc303a0ef24bf2ffc03619335643bf))
  - Drop diagnostics: WARN logs with table name and reason for every drop path ([84d621d](https://github.com/vnvo/deltaforge/commit/84d621d27ddc303a0ef24bf2ffc03619335643bf))
  - Provenance header (`df-source-kind: outbox`) preserved after sentinel clearing ([558a269](https://github.com/vnvo/deltaforge/commit/558a26954a30a2247d7b218ca0005f8795cf9b08))
  - Outbox metrics: `deltaforge_outbox_transformed_total`, `deltaforge_outbox_dropped_total{reason}` ([558a269](https://github.com/vnvo/deltaforge/commit/558a26954a30a2247d7b218ca0005f8795cf9b08))
  - Strict mode (`strict: true`) fails the batch on missing required fields instead of silent drops ([09f04b8](https://github.com/vnvo/deltaforge/commit/09f04b85aa94b602ebaabca9b16d13f5f06ecfa6))

### Changed

- Bumped pgwire-replication to 0.2 - logical decoding messages arrive as typed ReplicationEvent::Message

### Documentation


- Updated landing page: flatten processor icon in tech strip, flatten annotation in quickstart demo, production-focused features section overhaul ([487cde3](https://github.com/vnvo/deltaforge/commit/487cde33140db736159b72caee73c00de4727206))
- Updated processors documentation with full flatten config reference, max_depth trace example, collision policy explanation, outbox chaining example, and envelope interaction section ([579cff6](https://github.com/vnvo/deltaforge/commit/579cff6c1a5b8eab8e705dd5ff96911723fb636a))
- Updated configuration.md with flatten processor YAML snippet and config table
- Added outbox pattern documentation with full config reference, column mappings table, and Debezium migration guide ([cf501c8](https://github.com/vnvo/deltaforge/commit/cf501c871fa349acde83e301479cb40427fd9e14))
- Updated docs to cover outbox processor configuration ([5a5a75b](https://github.com/vnvo/deltaforge/commit/5a5a75b6128ae0295d97627f59c73405ff16e0f4))
- Updated landing page with outbox processor icon and feature card ([7431118](https://github.com/vnvo/deltaforge/commit/7431118ef0502ff9962912093a33927621cb0a99))
- Updated README and introduction with outbox in processors tech table, features list, and roadmap ([6522c33](https://github.com/vnvo/deltaforge/commit/6522c333e8a220f08eae632b9153bde4314907ae))
- Added observability section with metrics reference and drop reason table

### Testing

- Added FlattenProcessor unit tests (basic flattening, max_depth, all policy combinations, collision handling, outbox-style payloads, no-payload passthrough)
- Added config parsing tests for flatten processor covering defaults, all explicit policy variants, default id, and outbox+flatten chain ([0379a15](https://github.com/vnvo/deltaforge/commit/0379a157bf6174c3772000dbc807cfa11c47c1f1))
- Added flatten processor criterion benchmark ([6fe4e67](https://github.com/vnvo/deltaforge/commit/6fe4e676298d2ea49d52861ef1b011045e2fb4f2))
  - ~900K events/sec for typical nested payloads; recursion depth has negligible impact
  - 1.14M events/sec for already-flat payloads - fast path is effective
  - ~1.8M fields/sec on wide payloads - linear scaling, no cliff
  - Analytics config overhead negligible (~946K vs ~928K default)
  - Batch throughput ~757K events/sec at batch_500
- Added OutboxProcessor unit tests (transform, topic/key resolution, headers, strict mode, drop paths)
- Added outbox criterion benchmark (~650K events/sec single transform, linear batch scaling) ([6d2b772](https://github.com/vnvo/deltaforge/commit/6d2b772e6671145f91016e2b09c4f370f7ed8c35))
- Added PostgreSQL outbox e2e integration test (WAL message capture -> processor -> transformed event)
- Added MySQL outbox e2e integration test (BLACKHOLE table -> processor -> transformed event)

### Fixed

- **Batch byte cap default** - Corrected typo `3 * 1014 * 1024` → `3 * 1024 * 1024` (3MB) in `BatchConfig` default ([77b20bd](https://github.com/vnvo/deltaforge/commit/77b20bd56168afbbdde414a20c6640e9f7becc87))

### Performance

- **Flatten processor** benchmarked at ~900K events/sec for typical nested payloads, 1.14M events/sec for already-flat inputs; recursion depth adds negligible overhead
- No deep JSON clones in outbox processor - uses take/remove instead of clone ([dc8fa68](https://github.com/vnvo/deltaforge/commit/dc8fa683bd4a8559ad13e141df8a8b7eb9d841d5))
- Outbox processor benchmarked at ~650K events/sec per transform, 1.48M elem/sec for mixed CDC+outbox pipelines
- Key template resolution adds zero measurable overhead
- Additional headers (3 extra) add ~30% overhead
- **Concurrent sink delivery** - Sinks now receive each batch in parallel instead of sequentially ([6a6a5fb](https://github.com/vnvo/deltaforge/commit/6a6a5fbeec3375d151e471930dcc66dfa935f98a))
  - Latency scales with `max(sink_latencies)` instead of `sum(sink_latencies)`
  - 2 sinks at 150ms each: ~300ms/batch → ~150ms/batch; 3 sinks: ~450ms → ~150ms
  - Zero-copy delivery preserved — all sinks share the same `Arc<[Event]>` frozen batch
  - Commit policy (all/required/quorum) and per-sink metrics fully preserved
- **Non-blocking SQLite checkpoint store** - All SQLite operations dispatched via `tokio::task::spawn_blocking`, preventing Tokio worker thread stalls under load ([3af0d31](https://github.com/vnvo/deltaforge/commit/3af0d31e27fc6e9a69e8ca1715b535b989338e29))
  - WAL journal mode, `synchronous=NORMAL`, `busy_timeout=5000`, and `foreign_keys=ON` enabled for improved write throughput
  - `Arc<Mutex<Connection>>` replaces bare `Mutex<Connection>` to allow safe movement into blocking tasks

---

## [0.1.0-beta.7] - 2025-02-16

### Added

- **Dynamic event routing** - Route events to per-table destinations using template strings or JavaScript logic ([52a1d54](https://github.com/vnvo/deltaforge/commit/52a1d54acb0791202eda1d47b891c4727a566b15))
  - `CompiledTemplate` with zero-overhead static detection and per-event resolution
  - Template variables: `${source.table}`, `${source.db}`, `${op}`, `${after.<field>}`, `${before.<field>}`, `${tenant_id}`
  - New `key` field on Kafka, Redis, and NATS sinks for partition/message key control
  - Kafka: resolves topic and key per-event, passes headers to Kafka message headers
  - Redis: resolves stream and key per-event, delivers key as `df-key` and headers as `df-headers` field
  - NATS: resolves subject and key per-event, delivers key and headers as NATS headers
- **JavaScript routing API** - `ev.route()` for programmatic per-event routing in JS processors ([0c6efdae](https://github.com/vnvo/deltaforge/commit/0c6efdae23ca7f84ec4b5f47c7587e02d10bfe5d))
  - Override topic, key, and headers from JavaScript with `ev.route({ topic, key, headers })`
  - Resolution priority: `ev.route()` override → config template → static config value
- **High-cardinality key detection** - Automatic detection and normalization of dynamic map keys in schema sensing
  - Probabilistic classification using HyperLogLog and SpaceSaving algorithms
  - Stable vs dynamic field detection with configurable thresholds
  - Adaptive structure hashing that ignores dynamic key names
  - Reduces false schema evolutions from 100% to <1% for dynamic key payloads
  - New configuration options: `high_cardinality.enabled`, `min_events`, `stable_threshold`, `min_dynamic_fields`
  - REST API endpoint: `GET /pipelines/{name}/sensing/schemas/{table}/classifications`
- **Schema sensing metrics** - Prometheus metrics for cache effectiveness and performance monitoring
  - `deltaforge_schema_events_total` - Events observed per table
  - `deltaforge_schema_cache_hits_total` / `cache_misses_total` - Cache effectiveness
  - `deltaforge_schema_evolutions_total` - Schema changes detected
  - `deltaforge_schema_tables_total` / `dynamic_maps_total` - Gauge metrics
  - `deltaforge_schema_sensing_seconds` - Per-event latency histogram
- **Envelope formats** - Configurable output formats for sink messages ([71f4fdb](https://github.com/vnvo/deltaforge/commit/71f4fdb94e46a2dfa4cf56be43ea520246772126))
  - **Native**: Direct Event serialization with minimal overhead
  - **Debezium**: Wire-compatible with Debezium's schemaless mode (`{"schema":null,"payload":{...}}`)
  - **CloudEvents**: CNCF CloudEvents 1.0 specification support
  - Per-sink envelope configuration via `envelope.type` in YAML
- **Encoding module** - Pluggable wire encoding for sink output ([316d763](https://github.com/vnvo/deltaforge/commit/316d76397479e00e07fef56f8568b90cf48ec4fc))
  - JSON encoding (default)
  - Extensible design for future Avro/Protobuf support

### Changed

- **Config loader** - Lenient environment variable expansion; unknown `${...}` variables pass through as routing templates instead of erroring ([e2b185b](https://github.com/vnvo/deltaforge/commit/e2b185baf876fb0f6f8a98da6a710e0be7a1fbc7))
- **Event structure** - Adopted Debezium-compatible envelope as base event structure ([57f2b9c](https://github.com/vnvo/deltaforge/commit/57f2b9c16c9998aebc9f297d8e374207b1b8b7d9))
- **Sink configuration** - All sinks now support `envelope` and `encoding` options ([72a1898](https://github.com/vnvo/deltaforge/commit/72a1898dce913b8a6ff68ff0652224759d798854))
  - Kafka, Redis, and NATS sinks updated to use envelope/encoding pipeline
- **MySQL source** - Refactored to adopt envelope/encode model ([684b165](https://github.com/vnvo/deltaforge/commit/684b16575a9ae8f7958d0d1ffd70b8d0661aef1e))
- **PostgreSQL source** - Refactored to adopt envelope/encode model ([a9ac527](https://github.com/vnvo/deltaforge/commit/a9ac527308306df5f3fc4e4c26643358e17416b2))
- **Debezium envelope** - Uses `schema: null` for schemaless mode compatibility ([0e5cf38](https://github.com/vnvo/deltaforge/commit/0e5cf388b95ef02481f018af08d8f8cebd459c43))

### Fixed

- **MySQL DDL handling** - Fixed DDL event handling and required schema reload ([6dfe592](https://github.com/vnvo/deltaforge/commit/6dfe59204644e805167c7e474d5d11e052156386))
- **Sink unit tests** - Fixed broken unit tests related to sink routing changes ([c8e23cd](https://github.com/vnvo/deltaforge/commit/c8e23cd594121fb6d321b3fc755aaac327535678))

### Documentation

- Added dynamic routing documentation with template variable reference and resolution order ([cb6bafb](https://github.com/vnvo/deltaforge/commit/cb6bafb50c9cf2dd7c8c18d963209849ef83e358))
- Added example configs: `dynamic-routing.yaml` and `js-routing.yaml` ([81cf409](https://github.com/vnvo/deltaforge/commit/81cf409f02ff863b45674597ca70867b3b41e951))
- Added landing page for project documentation ([61f7c1b](https://github.com/vnvo/deltaforge/commit/61f7c1b23995ed2700096a6f35e05149bec074e1))
- Updated configuration reference with `key` field and routing template links
- Updated introduction, README, SUMMARY, and quickstart with routing feature
- Added high-cardinality key handling documentation with configuration examples
- Updated schema sensing docs with side-by-side configuration reference
- Added dynamic map classifications API documentation
- Updated README with high-cardinality feature highlights
- Added envelope formats documentation with wire format examples ([538eee6](https://github.com/vnvo/deltaforge/commit/538eee60c356f116bfc6818148f4a6c00325085f))
- Updated README with envelope configuration and quick start guide ([d39f455](https://github.com/vnvo/deltaforge/commit/d39f455aca56859651206985c75394efcd5d4522))
- Updated example configurations with envelope options

### Testing

- Added integration tests for dynamic routing across Kafka, Redis, and NATS sinks ([75dae63](https://github.com/vnvo/deltaforge/commit/75dae637f289af40695b3791d9f0cb96437635ef))
- Added JavaScript routing override tests ([aed8b59](https://github.com/vnvo/deltaforge/commit/aed8b59995a1396c8f3a1e3319f4d1e692d52496))
- Added config parsing tests for `key` field and template topic preservation
- Refactored sink integration tests with shared event factories
- Updated all sink integration tests for envelope and encoding support ([6938491](https://github.com/vnvo/deltaforge/commit/693849117d163c944749435f7bc2c0bdb2307b16))
- Removed Turso integration tests temporarily ([17d933c](https://github.com/vnvo/deltaforge/commit/17d933c9869886f2f53d54de71423933d82db607))
- Added high-cardinality detection effectiveness tests
- Added schema sensing benchmarks with Criterion

### Performance

- **Schema sensing optimizations** - Reduced overhead for high-cardinality detection
  - Fast path for pure structs (no dynamic fields) - near-zero overhead after warmup
  - Confidence scaling fix eliminates classification retry loop
  - Pre-sorted stable fields with binary search (removes per-object HashSet allocation)
  - Single-probe structure cache (removes double hash lookup)
  - Lazy classifier updates with graduated sampling during warmup

---

## [0.1.0-beta.6] - 2025-01-11

### Added

- **NATS sink** - JetStream sink with durable delivery ([21d7800](https://github.com/vnvo/deltaforge/commit/21d7800f2604570ad305e573b9353929cc256ad6))
  - JetStream stream verification at startup
  - Multiple authentication methods (credentials file, username/password, token)
  - Configurable timeouts for send, batch, and connection
  - Comprehensive integration tests
- **PostgreSQL source** - Full CDC support via logical replication with pgoutput plugin ([72ac4bc](https://github.com/vnvo/deltaforge/commit/72ac4bc3d30bae608a1dc4f7c8762cfbaaf4e731))
  - Automatic slot and publication creation
  - LSN-based checkpointing with configurable start position
  - Wildcard table patterns (`schema.*`, `schema.prefix%`)
  - Proper handling of PostgreSQL arrays and JSONB types
  - Connection retry with exponential backoff
- **Sink resilience** - Retry logic with exponential backoff for Kafka and Redis sinks
  - Connection pooling for Redis sink with automatic reconnection
  - Error classification for smart retry decisions (auth errors fail fast, transient errors retry)
  - Graceful shutdown via `CancellationToken`
- **Configurable sink timeouts** - New configuration options:
  - `KafkaSinkCfg.send_timeout_secs` - per-message send timeout (default: 30s)
  - `RedisSinkCfg.send_timeout_secs` - per-XADD timeout (default: 5s)
  - `RedisSinkCfg.batch_timeout_secs` - pipeline batch timeout (default: 30s)
  - `RedisSinkCfg.connect_timeout_secs` - connection establishment timeout (default: 10s)
- **Sink integration tests** - Comprehensive testcontainers-based tests for Redis and Kafka sinks
  - Connection recovery after restart
  - Connection drop handling
  - Cancellation token respect
  - Large payload and concurrent access tests
- **Version information** - Build version, git commit, and target displayed at startup ([de27d74](https://github.com/vnvo/deltaforge/commit/de27d748fff5c822d53a5581cdd6aa3438bf9d1a))
- JavaScript processor timeout and health monitoring ([11a89bc](https://github.com/vnvo/deltaforge/commit/11a89bcc80b15ac7e1b1dd43254a1fba9f28b953))
- Turso/libSQL source (experimental, behind feature flag) ([729bfe7](https://github.com/vnvo/deltaforge/commit/729bfe71cf1cefe434b6fbcf432c65ba621b5284))

### Documentation

- Updated sink documentation with use cases, pros/cons, and failure modes ([39e32c7](https://github.com/vnvo/deltaforge/commit/39e32c74837e7a7bb3e33488b525baa802ea6a85))
- Added NATS sink documentation
- Expanded multi-sink configuration guide with `required` flag and commit policy details

### Changed

- **`build_sinks()` signature** - Now requires `CancellationToken` parameter for graceful shutdown
- **`common` crate exports** - Added `is_retryable_message`, `is_permanent_failure`, `Retryable` trait, `PauseResult`, and additional time utilities
- Coordinator now owns checkpoint saving (ensures events reach sinks before checkpointing) ([54c98b2](https://github.com/vnvo/deltaforge/commit/54c98b27ccee77cba3f15bac91ef96c29fb1862d))
- Turso source hidden behind `turso` feature flag ([696d840](https://github.com/vnvo/deltaforge/commit/696d840da1c17e295c3cc7ed441faf7b6066c83c))
- Cleaned up retry policy with better default parameters ([8549899](https://github.com/vnvo/deltaforge/commit/8549899271d547ca247ec9e4440ae852dea75cc0))
- Events no longer serialize checkpoint and size_bytes fields ([9903c69](https://github.com/vnvo/deltaforge/commit/9903c69ce0a35ca9eb304b6c76a6b74192550c30))

### Fixed

- **Credential safety** - All sink connection strings are now redacted in logs
- PostgreSQL boolean array handling ([c6dc3c4](https://github.com/vnvo/deltaforge/commit/c6dc3c4c07fd4cf23caf5a18d25c5f51ec1219cb))
- MySQL binlog_row_image validation now warns if not set to FULL ([39c86f4](https://github.com/vnvo/deltaforge/commit/39c86f46c3734c381018de8ffe0d2f6c10bdc7a4))
- MySQL source retry handling ([501a418](https://github.com/vnvo/deltaforge/commit/501a418b2e8e844022c90095d62a669ea4148bf5))
- JavaScript processor error propagation ([2ba29d1](https://github.com/vnvo/deltaforge/commit/2ba29d18ff6690c645c8181a4469f5118231514c))
- Checkpointing now handled by coordinator ([54c98b2](https://github.com/vnvo/deltaforge/commit/54c98b27ccee77cba3f15bac91ef96c29fb1862d))

### Refactoring

- **Shared utilities (`common` crate)** - Consolidated common functionality ([e6b1eb9](https://github.com/vnvo/deltaforge/commit/e6b1eb9e77afcf1ae3a8bd778a8c0773d354726f))
  - DSN parsing and credential redaction
  - Retry logic with exponential backoff
  - Pattern matching for tables/topics
  - Pause gates and async utilities
- MySQL source updated to use common utilities ([066d0b3](https://github.com/vnvo/deltaforge/commit/066d0b31b3a9540fa7e96defd781ff08ca24f766))
- PostgreSQL source updated to use common utilities ([526b042](https://github.com/vnvo/deltaforge/commit/526b042ffe3cd4f2a64a8bf69b644cbbee2af507))
- Removed deprecated `conn_utils` module ([ad9c2ef](https://github.com/vnvo/deltaforge/commit/ad9c2efc33d88cf6217adf93cd123e0cd5bd562c))
- Sinks updated to use common utilities ([85b411e](https://github.com/vnvo/deltaforge/commit/85b411e53938948a1759855ddf4d11370e9f8781))

### Infrastructure

- Docker images now include SSL certificates ([6e129fc](https://github.com/vnvo/deltaforge/commit/6e129fc7031fd76c6bf37c811fa00beb9c1361b4))
- Debug Docker image includes libssl and libsasl2 ([a1785c2](https://github.com/vnvo/deltaforge/commit/a1785c2a3795f87aa67bb4df96fc7c99a0969ceb))
- Applied workspace-level linting configuration ([7d23d5f](https://github.com/vnvo/deltaforge/commit/7d23d5f09bdef06a6a4da305542c406a1dddb47e))

---

## [0.1.0-beta.5] - 2025-12-26

### Added

- **Schema registry** - Extended with versioning and multiple backend options ([ba95c95](https://github.com/vnvo/deltaforge/commit/ba95c95a5ad064cf5f6efb383bf275b3c7c0279d))
  - SHA-256 fingerprinting for stable change detection ([40f750e](https://github.com/vnvo/deltaforge/commit/40f750e9076c7dc8e5ab96150b1f3aa3d1f844a1))
  - Schema sequence tracking from source ([333aaa3](https://github.com/vnvo/deltaforge/commit/333aaa3451e0bc00437396aa037bb1f1fd4dda26))
  - REST API endpoints for schema inspection ([b9fed82](https://github.com/vnvo/deltaforge/commit/b9fed8267f003337e727aac65a2805eb03fb7697))
- **Schema sensing** - Automatic schema inference from JSON event payloads ([8ccea69](https://github.com/vnvo/deltaforge/commit/8ccea6984262dbdb9621786616213c9253cf7a9c))
  - Deep inspection of nested JSON structures
  - Configurable sampling and caching
  - Drift detection capabilities
  - REST API for inferred schemas and statistics ([7a874de](https://github.com/vnvo/deltaforge/commit/7a874dece99c128ac4b34709a053795ed01393ee))
- **Turso source** - Initial CDC support for Turso/libSQL databases ([f51b3ac](https://github.com/vnvo/deltaforge/commit/f51b3acf797cbc9c45c4c1bd1a5bc0143e413c5f))
- Batch send API for sinks (removes unnecessary clones) ([8d923d9](https://github.com/vnvo/deltaforge/commit/8d923d95210608a7862e9303678c273a4f1b7fdc))
- Sink `required` field configuration for commit policy ([9a1cd95](https://github.com/vnvo/deltaforge/commit/9a1cd9571b5eef2aed6c7654379e4949d40f21e7))
- Pipeline GET and DELETE endpoints ([cc4d724](https://github.com/vnvo/deltaforge/commit/cc4d724ec581754433ec29c2e1cb77d15095aa7b))
- Prometheus metrics guide ([6f583ea](https://github.com/vnvo/deltaforge/commit/6f583ea7b5b00997e9a971500de079fc2a659001))

### Changed

- Pipeline manager refactored with cleaner controller separation ([89e2bb2](https://github.com/vnvo/deltaforge/commit/89e2bb2457606a4acf561dbe24ff9d69d18ff33d))
- Optimized schema sensing integration in critical path ([2c8eb22](https://github.com/vnvo/deltaforge/commit/2c8eb22eac76f47c0bf7b5baf9e1d759c78feec9))
- Event size calculation optimized ([a8de2f2](https://github.com/vnvo/deltaforge/commit/a8de2f2ac528eb639e2c8999b7fcb2bb7640129e))

### Fixed

- Checkpoint inconsistency between source and runner ([d9c3442](https://github.com/vnvo/deltaforge/commit/d9c3442a01dddb9a77b4fccaba47c2eac288f682))
- rusqlite usize compatibility ([6b045e4](https://github.com/vnvo/deltaforge/commit/6b045e43dd25a167715458ebc12d1641fcbb9680))

### Performance

- Use `Arc<[u8]>` for CheckpointMeta ([ff2d84a](https://github.com/vnvo/deltaforge/commit/ff2d84a2059f36e7f3a2808d97aa3afcd1ce5b4a))

---

## [0.1.0-beta.4] - 2025-12-14

### Added

- **Multi-architecture Docker images** - Support for linux/amd64 and linux/arm64 ([e7c5f77](https://github.com/vnvo/deltaforge/commit/e7c5f777c7b05dbd9d9fc5f6576774806335eeef))
  - Minimal image (~57MB, scratch-based)
  - Debug image (~140MB, includes shell and debugging tools)

---

## [0.1.0-beta.2] - 2025-12-14

### Fixed

- Missing zlib dependency in release container ([e8317fa](https://github.com/vnvo/deltaforge/commit/e8317fab366db0f76fbc100d60a3d382db22ce9c))

---

## [0.1.0-beta.1] - 2025-12-13

### Added

- **Core CDC engine** - Initial release with MySQL binlog support
- **MySQL source** - Binlog-based CDC with GTID support ([95f4503](https://github.com/vnvo/deltaforge/commit/95f45030fde2a4757ab4aff2f73475b76bbb6f00))
  - Row-level change capture (INSERT, UPDATE, DELETE)
  - GTID-based checkpointing ([a72ac57](https://github.com/vnvo/deltaforge/commit/a72ac5736c517c1fb884773a57f638cae5328cf6))
  - Automatic reconnection with retry policy
  - Table filtering with wildcard patterns
- **Kafka sink** - Event streaming to Apache Kafka ([a70135f](https://github.com/vnvo/deltaforge/commit/a70135fe95af35cabc8294c3432495b961c4310f))
  - Exactly-once semantics support ([1b74f5a](https://github.com/vnvo/deltaforge/commit/1b74f5a4cf389807cf3cbe76a1f01ab52b0f0eee))
  - Client configuration overrides ([c6ce2fc](https://github.com/vnvo/deltaforge/commit/c6ce2fcda95bbe327e4706656ed82635e3cb73ce))
- **Redis sink** - Event streaming to Redis Streams ([4508bba](https://github.com/vnvo/deltaforge/commit/4508bbae89ed4f1391219e3d1b26984e4b62d549))
- **JavaScript processor** - Transform events with embedded JavaScript ([b0a3f9a](https://github.com/vnvo/deltaforge/commit/b0a3f9a5516c74aed8fc32dff8072c6c9c5ccb0c))
  - Batch processing with persistent runtime ([cf924aa](https://github.com/vnvo/deltaforge/commit/cf924aa2910538465ea25dee79329c24b92871c6))
  - In-place mutation and event duplication support ([55337e3](https://github.com/vnvo/deltaforge/commit/55337e378d93f499df78ee3cab6ba5bb8fd35d32))
- **Pipeline coordinator** - Batching and multi-sink orchestration ([21f01a5](https://github.com/vnvo/deltaforge/commit/21f01a591e1829ec8f8c92117181e78d73d1a2fc))
  - Configurable batch size, byte limit, and time window
  - Transaction boundary preservation
  - Commit policies (all, required, quorum)
- **REST API** - Pipeline management and health endpoints ([57d2823](https://github.com/vnvo/deltaforge/commit/57d282365ee9d2b8d9a73b276d7c4790dc7c8674))
  - Health and readiness probes
  - Pipeline pause/resume/stop controls
  - Prometheus metrics endpoint
- **Configuration** - YAML-based pipeline definitions ([67156c9](https://github.com/vnvo/deltaforge/commit/67156c9ab4676f1ee261d99043d2c3e97ccee0d9))
  - Environment variable expansion (`${VAR}`)
  - Directory-based config loading
- **Observability** - Structured logging and metrics ([70e08d2](https://github.com/vnvo/deltaforge/commit/70e08d23daa3a723853303ea6d4252fe7646dae5))
  - JSON structured logs
  - Prometheus metrics integration ([da540a7](https://github.com/vnvo/deltaforge/commit/da540a7bfe6b03696d8d88c8f230d482cce63f82))
- **Checkpointing** - Persistent position tracking ([b6619c8](https://github.com/vnvo/deltaforge/commit/b6619c8ceccf9705638a718f71590806fc7c12dc))
  - Memory-backed checkpoint store
  - SQLite-backed checkpoint store
- **Documentation** - mdBook-based user guide ([178a413](https://github.com/vnvo/deltaforge/commit/178a4130c6d43999a161e69a6c76abd05d9ae972))
- **CI/CD** - GitHub Actions workflows
  - Build and test pipeline ([b01ea7d](https://github.com/vnvo/deltaforge/commit/b01ea7de3149786eefc2ac725c6e692ceab0a2eb))
  - Code coverage reporting ([89c0033](https://github.com/vnvo/deltaforge/commit/89c0033b13e91d662021f1bc1d2957de0182ddb0))
  - Docker image releases ([a2d19d8](https://github.com/vnvo/deltaforge/commit/a2d19d8e71e5002ef425b51c79bacf54b3a42d21))
  - Documentation deployment

### Changed

- Switched to Rust Edition 2024 ([ee976c9](https://github.com/vnvo/deltaforge/commit/ee976c9212fff588fb4f2f7efdc3b53e9942eeed))
- Adopted MIT OR Apache-2.0 dual license ([5eb008a](https://github.com/vnvo/deltaforge/commit/5eb008a11fe1cb7ac15b5b82289a21f938b849a0))

---

## Links

- [Unreleased]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.7...HEAD
- [0.1.0-beta.7]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.6...v0.1.0-beta.7
- [0.1.0-beta.6]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.5...v0.1.0-beta.6
- [0.1.0-beta.5]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.4...v0.1.0-beta.5
- [0.1.0-beta.4]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.2...v0.1.0-beta.4
- [0.1.0-beta.2]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.1...v0.1.0-beta.2
- [0.1.0-beta.1]: https://github.com/vnvo/deltaforge/releases/tag/v0.1.0-beta.1