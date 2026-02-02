# Changelog

All notable changes to DeltaForge will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

- **Event structure** - Adopted Debezium-compatible envelope as base event structure ([57f2b9c](https://github.com/vnvo/deltaforge/commit/57f2b9c16c9998aebc9f297d8e374207b1b8b7d9))
- **Sink configuration** - All sinks now support `envelope` and `encoding` options ([72a1898](https://github.com/vnvo/deltaforge/commit/72a1898dce913b8a6ff68ff0652224759d798854))
  - Kafka, Redis, and NATS sinks updated to use envelope/encoding pipeline
- **MySQL source** - Refactored to adopt envelope/encode model ([684b165](https://github.com/vnvo/deltaforge/commit/684b16575a9ae8f7958d0d1ffd70b8d0661aef1e))
- **PostgreSQL source** - Refactored to adopt envelope/encode model ([a9ac527](https://github.com/vnvo/deltaforge/commit/a9ac527308306df5f3fc4e4c26643358e17416b2))
- **Debezium envelope** - Uses `schema: null` for schemaless mode compatibility ([0e5cf38](https://github.com/vnvo/deltaforge/commit/0e5cf388b95ef02481f018af08d8f8cebd459c43))

### Fixed

- **MySQL DDL handling** - Fixed DDL event handling and required schema reload ([6dfe592](https://github.com/vnvo/deltaforge/commit/6dfe59204644e805167c7e474d5d11e052156386))

### Documentation

- Added high-cardinality key handling documentation with configuration examples
- Updated schema sensing docs with side-by-side configuration reference
- Added dynamic map classifications API documentation
- Updated README with high-cardinality feature highlights
- Added envelope formats documentation with wire format examples ([538eee6](https://github.com/vnvo/deltaforge/commit/538eee60c356f116bfc6818148f4a6c00325085f))
- Updated README with envelope configuration and quick start guide ([d39f455](https://github.com/vnvo/deltaforge/commit/d39f455aca56859651206985c75394efcd5d4522))
- Updated example configurations with envelope options

### Testing

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

- [Unreleased]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.6...HEAD
- [0.1.0-beta.6]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.5...v0.1.0-beta.6
- [0.1.0-beta.5]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.4...v0.1.0-beta.5
- [0.1.0-beta.4]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.2...v0.1.0-beta.4
- [0.1.0-beta.2]: https://github.com/vnvo/deltaforge/compare/v0.1.0-beta.1...v0.1.0-beta.2
- [0.1.0-beta.1]: https://github.com/vnvo/deltaforge/releases/tag/v0.1.0-beta.1