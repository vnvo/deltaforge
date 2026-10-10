# MySQL canonical types

Status: **proposal for review** (docs only). Nothing here is implemented.

## Rulings incorporated

- **MySQL JSON:** carried as canonical JSON text in a string.
- **TIMESTAMP:** an RFC 3339 UTC string in native JSON.
- **Wrong outputs:** corrected unconditionally for rc.1.
- **`type_mapping: v2`:** the rc.1 default. A legacy `v1` may only keep differences that are lossless.
- **Separate product changes:** the Avro converter's fail-closed rules and the S3 key length (a release blocker).
- **Second review:**
  - the mapping version travels with every event (section 5);
  - one failure model for the DLQ and checkpoints (section 6);
  - wide values are safe in native JSON (BIGINT UNSIGNED as a string by default, BIT(n>1) as fixed-width bytes);
  - ENUM index 0 and SET semantics are explicit;
  - the evidence is committed with checksums, and the images are pinned by digest.
- **Third review (architecture approved with these):**
  - `json_wide_integers: string` (default) renders BIGINT and BIGINT UNSIGNED as exact decimal strings in native JSON only (section 3);
  - events carry canonical logical values plus the mapping version, and encoders render them (section 5);
  - `enum_error_sentinel` is removed: ENUM index 0 fails closed in v1 and v2.

DDL work, T1a, the release gate and rc.1 stay held until this proposal is reviewed.

## 1. Evidence

| Item | Location |
|---|---|
| Probe | `crates/runner/tests/mysql_type_probe.rs`. An excluded measurement tool: it asserts nothing until it becomes the parity suite (slice 4). |
| Report generator | `scripts/type-probe-report.py <dir>` |
| Committed evidence | `docs/design/evidence/mysql-type-probe/`: for each run, the sanitized `report.md`, the environment manifest `env.json` and `SHA256SUMS` of every raw file the run produced. The raw files themselves are kept by the author; rerunning the pinned probe regenerates equivalent files. |
| Main run | `main/` (S3 `legacy_rolling`, so the Parquet encoder is observable) |
| Durable S3 run | `durable-s3/` (`TYPE_PROBE_S3_DURABILITY=durable_v2`; every Parquet pipeline fails, section 8.2) |

**Reproducing:** `cargo test -p runner --test mysql_type_probe -- --include-ignored --nocapture` (Docker). Output goes to `target/type-probe/<UTC time>/`, then run `scripts/type-probe-report.py` on it. All container images are pinned by digest in the probe and recorded in `env.json`:

| Image | Pinned reference |
|---|---|
| MySQL | `mysql:8.4@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084` |
| Kafka | `confluentinc/cp-kafka:7.5.0@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41` |
| Schema Registry | `confluentinc/cp-schema-registry:7.5.0@sha256:e51684b472a2481f065f44616d3d8ad2182029a7011f949a612d35b54566a1f6` |
| RustFS | `rustfs/rustfs:1.0.1@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c` |

**How the probe works:** real pipelines built by the pipeline manager from a live MySQL source, one per (sink, table):
- **Sinks:** Kafka JSON (native envelope), Kafka Avro (Confluent Schema Registry, DDL-derived schemas) and S3 Parquet (RustFS).
- **Coverage:** every value goes through a snapshot row, then a CDC insert, an update (before and after images) and a delete (before image).
- **Runs:** `binlog_row_metadata` MINIMAL and FULL, each with snapshot `initial` and `never`. `never` is needed because the typed sinks halt on snapshot rows.

**Environment** (`env.json`):

| Setting | Value |
|---|---|
| MySQL | 8.4.9 Community (pinned above) |
| SQL mode | strict default (`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`). Zero-date and invalid-enum rows were written with session `sql_mode=''`; the mode is recorded per statement. |
| Character set / collation | server `utf8mb4` / `utf8mb4_0900_ai_ci`; client, connection and results `utf8mb4`. Probed columns: `utf8mb4`, `latin1`, `ascii`, binary. |
| Time zones | server global and session `+02:00` (set on purpose); `system_time_zone` UTC; DeltaForge process `+03:00`. |
| Binlog | ROW, image FULL, GTID on |
| DeltaForge | recorded per run in `env.json` (`deltaforge_revision`) |

**Refused by strict MySQL** (no behavior to map):
- non-finite DOUBLE (`'NaN'`, `1e309`);
- out-of-range integers and decimals;
- `'2023-02-29'`;
- zero dates and invalid ENUM values in strict mode.

**Accepted with warnings in permissive mode:** invalid ENUM (stored as the empty error value, index 0) and invalid SET members (dropped).

## 2. Current behavior (measured)

Every CDC path agreed with itself. Each value was identical across insert, update and delete images and across MINIMAL and FULL metadata, so `binlog_row_metadata` changes no value. The defects are between paths and inside them.

| Type | Snapshot JSON | CDC JSON | Avro (CDC) | Parquet (CDC) |
|---|---|---|---|---|
| Integers incl. unsigned | decimal **string** | exact number | exact | exact |
| DECIMAL | exact string | exact string | exact string | Decimal128 (p <= 38); Utf8 above |
| FLOAT | MySQL text `"1.1"` | widened `1.100000023841858` | float | Float32 |
| BIT(n) | raw bytes, or base64 when not UTF-8 (data-dependent) | number | **ASCII digits** (`b'0'` encoded as `0x30`); record fails | fails |
| VARCHAR utf8mb4 / ascii | string | string | string | Utf8 |
| VARCHAR / TEXT latin1 | decoded string | `{"_base64"}` of the latin1 bytes | **mojibake** | **literal `{"_base64":...}` text** |
| TEXT utf8mb4 | string | `{"_base64"}` | string | **literal `{"_base64":...}` text** |
| BINARY(n) | zero-padded | **padding stripped** | padding stripped | padding stripped |
| VARBINARY / BLOB | string or base64 (data-dependent) | VARBINARY: same rule; BLOB: base64 | bytes | Binary |
| ENUM / SET | labels | **index / bitmask** | **index digits** | **index digits** |
| JSON | MySQL JSON text | embedded value; **JSON `null` becomes SQL NULL** | compact text | compact text |
| DATE | `"YYYY-MM-DD"`; zero `"0000-00-00"` | same; zero `"0-00-00"` | **silently 0 (epoch)** | fails |
| TIME | column precision | always 6 digits | fails | **unsupported (Time32)** |
| DATETIME | column precision | always 6 digits; zero `"0-00-00 ..."` | string | Utf8 |
| TIMESTAMP | wall clock in the snapshot session's zone, **no offset** | UTC epoch microseconds; zero becomes `0` | declared millis, value micros (**1000x**; from code, rows failed earlier) | declared ms (same mismatch) |
| YEAR | `"0000"`..`"2155"` | **0000 becomes 1900** | int | Int32 |

**Snapshot rows break the typed sinks.** They carry every value as a string, including primary keys, over the text protocol. Avro fails on the first snapshot batch of every table, and Parquet on every table with a numeric, bit or temporal column.

## 3. Canonical mapping (v2, the rc.1 default)

**Principles:**
- The **schema decides representation:** the column's definition in the schema version the row was decoded with. A value's bytes never do.
- **Snapshot and CDC produce the same internal value** and the same serialized output.
- A value that cannot be represented exactly in a target **fails closed** (section 6). There is no substitution, rounding, truncation, epoch or empty fallback.

| MySQL type | Internal logical value | Native/JSON | Avro | Arrow / Parquet |
|---|---|---|---|---|
| TINYINT, SMALLINT, MEDIUMINT, INT | i64 | number | int (INT UNSIGNED: long) | Int32 (INT UNSIGNED: Int64) |
| BIGINT | i64 | **exact decimal string** (`json_wide_integers: string`, default); a number under `json_wide_integers: number` | long | Int64 |
| BIGINT UNSIGNED | u64 | **exact decimal string** (`json_wide_integers: string`, default); under `json_wide_integers: number`, a number through i64::MAX and a sink conversion failure above | `bigint_unsigned`: string (default) or long (sink conversion failure above i64::MAX) | `bigint_unsigned`: Utf8 (default) or Int64 (same failure) |
| TINYINT(1) / BOOLEAN | i64 (MySQL stores the full range) | number | int | Int32 |
| DECIMAL(p,s) | exact decimal, scale s | string, MySQL form with s digits | string | Decimal128(p,s) for p <= 38; Utf8 for p > 38 (unchanged in rc.1) |
| FLOAT | f32 | number, shortest f32 round-trip (`1.1`) | float | Float32 |
| DOUBLE | f64 | number, shortest round-trip | double | Float64 |
| BIT(1) | boolean | `true` / `false` | boolean | Boolean |
| BIT(n>1) | the n bits as fixed-width bytes: ceil(n/8) bytes, big-endian, leading zero bits kept | `{"_base64":"..."}` of exactly ceil(n/8) bytes | `fixed`, size ceil(n/8) | FixedSizeBinary(ceil(n/8)) |
| CHAR, VARCHAR, TEXT family | Unicode text decoded from the column's charset; CHAR trailing spaces removed (MySQL default) | string | string | Utf8 |
| BINARY(n) | exactly n bytes, zero-padded as MySQL stores them | `{"_base64":"..."}` always | bytes | Binary |
| VARBINARY, BLOB family | bytes as stored | `{"_base64":"..."}` always | bytes | Binary |
| ENUM | (stored index, resolved label) from the decoding schema's definition | the label (indexes 1..N); index 0: see the ENUM and SET rules | string; `enum_mode: enum` gives an Avro enum, and a label that is not a valid Avro symbol fails closed | Utf8 |
| SET | (stored bitmask, member labels in definition order) | string `"x,z"` (MySQL's form); the empty set is `""` | string | Utf8 |
| JSON | MySQL JSON document | **semantic string of canonical JSON text**: SQL NULL is `null`; JSON null is `"null"`; objects, arrays, numbers, booleans and strings are their canonical text | string (same text) | Utf8 (same text) |
| DATE | civil date, or a MySQL zero/partial-zero date | `"YYYY-MM-DD"`; zero/partial: MySQL literal (`"0000-00-00"`) | int `date`; zero/partial fails closed | Date32; zero/partial fails closed |
| TIME(fsp) | signed duration, microseconds, -838:59:59 to 838:59:59 | `"[-]H+:MM:SS[.f{fsp}]"` | long microseconds, no logicalType (Avro time types are 0..24 h); property `deltaforge.logical: mysql-time-micros` | Duration(Microsecond) |
| DATETIME(fsp) | civil date-time, no zone | `"YYYY-MM-DDTHH:MM:SS[.f{fsp}]"` (ISO 8601, no zone designator); zero: MySQL literal `"0000-00-00 00:00:00"` | string (default `naive_timestamp_mode`) or `local-timestamp-micros`; zero fails closed in the typed form | Utf8 or Timestamp(Microsecond, none); zero fails closed |
| TIMESTAMP(fsp) | UTC instant, microseconds | **RFC 3339 UTC with Z and the column's precision** (`"2024-03-30T23:30:00Z"`, `"2024-10-27T00:30:00.654321Z"`); zero: MySQL literal `"0000-00-00 00:00:00"` | long `timestamp-micros`; zero fails closed | Timestamp(Microsecond, "UTC"); zero fails closed |
| YEAR | 0 or 1901..2155 | number (0 for 0000) | int | Int32 |
| NULL | null | null | null branch of the union | null |

**JSON canonical text** is the text MySQL itself returns for the document (`CAST(j AS CHAR)`): key order, separators, number forms and string escapes.
- CDC renders the binary JSONB with those rules, so CDC text equals snapshot text byte for byte.
- Numbers stay exact: an integer beyond 2^53, `1.0` and `1e300` keep MySQL's text and never pass through f64.
- Opaque JSONB values (DECIMAL, DATE, TIME, DATETIME inside JSON) render as MySQL renders them. Anything else fails closed.

**Binary wrapper:** binary values in native JSON are exactly `{"_base64":"<standard base64 with padding>"}`, one key, always used for binary types, never for text.

**Wide integers in native JSON (`json_wide_integers`, native JSON only):**

```yaml
type_mapping: v2
json_wide_integers: string   # default
```

- **`string` (default):** BIGINT and BIGINT UNSIGNED are exact decimal strings, so every value survives ordinary JSON parsing.
- Smaller integer types (TINYINT through INT, signed and unsigned) stay JSON numbers: their full ranges are below 2^53.
- **`number`:** restores numeric BIGINT output for consumers whose JSON parsers handle 64-bit integers. BIGINT UNSIGNED values above i64::MAX are then a sink conversion failure.
- **Interoperability risk of `number`:** JavaScript, `jq`, many JSON libraries and anything that parses JSON numbers as IEEE doubles silently round integers above 2^53 (9007199254740992), with no error. The configuration reference and the migration guide state this next to the setting.
- **Typed sinks are unaffected:** Avro and Arrow/Parquet keep their signed Int64 for BIGINT. The existing `bigint_unsigned` policy (alias: Avro `unsigned_bigint_mode`; conflicting settings are rejected at config validation) still decides string or signed long for BIGINT UNSIGNED in typed sinks.

**ENUM and SET rules:**
- **ENUM:** the internal value carries both the stored index and the resolved label.
  - Indexes 1..N emit their declared label. A declared empty label `''` is a valid label, emitted as `""`.
  - **Index 0 is MySQL's error sentinel** (written for an invalid value in non-strict mode). It is never equivalent to a declared `''`.
  - **Index 0 is a source decoding failure in both v1 and v2** (section 6). rc.1 has no option to emit it: a sentinel-only wrapper would make the column alternate between a string and an object depending on the value, against the schema-driven rule.
  - If sentinel emission is needed later, it comes as a separately versioned representation in which every ENUM value uses one stable tagged structure.
  - An index above N (a definition the decoding schema does not have) is always a source decoding failure.
- **SET:** the internal value carries the stored bitmask and its labels.
  - Invalid members are discarded by MySQL at write time (non-strict mode). The stored value contains only valid members, so there is nothing to detect and the emitted set is exactly what MySQL stores.
  - A bit outside the decoding schema's definition is a source decoding failure.

## 4. Temporal and charset rules

**Temporal values:**
- **TIMESTAMP is an instant.** CDC reads the stored epoch value. The snapshot session runs with `time_zone = '+00:00'`, so no session setting can change the instant.
- **DATETIME is civil:** never converted, never given a zone.
- **DATE and TIME keep their own categories.** TIME is a signed duration, not a time of day.
- **Precision:** the declared fractional precision (fsp 0..6) decides how many digits appear in text, in both paths. Typed sinks use microseconds; a millisecond logical type with microsecond values is a defect.
- **Zero and invalid values** (MySQL zero dates, zero-in-date parts, TIMESTAMP zero) are the MySQL literal text in native JSON. In typed sinks they fail closed per section 6. They never become an epoch or null.
- **DST and offsets:** the server zone affects only how MySQL interpreted the SQL literal on write. Output is UTC (TIMESTAMP) or civil (DATETIME), independent of the server, session and DeltaForge process zones.

**Character sets:**
- **Text** is decoded from the column's character set (from the decoding schema's column metadata) to Unicode, in both paths. CDC receives the stored bytes; the snapshot reads with `character_set_results = binary` and decodes with the same decoder.
- **Undecodable bytes** for the declared charset fail closed. They are never replaced or base64-wrapped.
- **Binary types** are bytes. Their text rendering is always `{"_base64"}`, whatever the bytes are.

## 5. Mapping versions: per event, v1, v2 and defaults

`type_mapping` is set per pipeline in the source config.
- **`v2` (default for rc.1, new and existing pipelines):** section 3.
- **`v1` (temporary, legacy)** differs from v2 only where the old output was **lossless**:
  - native JSON TIMESTAMP as epoch microseconds (a number);
  - TIME/DATETIME with six fractional digits regardless of fsp;
  - FLOAT rendered as the widened f64 decimal;
  - BIGINT, BIGINT UNSIGNED and BIT(n>1) as JSON numbers (`json_wide_integers` is a v2 setting).
- **No mode reproduces corruption** (section 7 list) or a data-dependent type. v1 gets every correction too.
- v1 logs a deprecation warning at start and is removed after rc.1's support window.

**The version travels with every event.** A pipeline-level setting or a restart marker is not enough: optional sinks, replay and the DLQ process events long after a restart.
- **Stamping:** the source emits **canonical logical values** (the internal column of section 3: exact integers, decimals, f32/f64, decoded text, bytes, ENUM index and label, SET bitmask and labels, canonical JSON text, temporal values in microseconds with their fsp) and stamps the event with the mapping version, before it enters batching, the replay journal or the DLQ. Events never carry a sink rendering.
- **Rendering:** every encoder (native JSON included) renders the canonical values according to the event's mapping version and its own sink settings (`json_wide_integers`, `bigint_unsigned`, `enum_mode`, `naive_timestamp_mode`). The source is therefore not coupled to any one sink, and v1 and v2 can be rendered from the same event.
- **Encoding:**
  - Encoders use the **event's** version, never the pipeline's current setting.
  - A batch holding more than one version is split at the version boundaries before encoding.
- **Replay and DLQ:** the replay journal and the DLQ persist the lossless canonical event with its version, never a rendered form.
  - Per-sink replay, the replay journal and DLQ records preserve each event's version. A lagging optional sink that replays v1 events after the pipeline switched to v2 encodes them as v1.
  - The S3 writer identity (and encoding domain) and the Avro schema cache key and subject include the version. v1 and v2 records never share a file, a cached schema or a subject.
- **Restart marker:** a marker event at a switch is informational. It never replaces per-event stamping.
- **Downgrade:** a binary that meets an event with a version it does not support fails closed with an incident. It never encodes it with another version.

## 6. Failure model

Two failure classes, each with one rule.

**Source decoding failures (fail closed, never DLQ-skippable):**
- **Causes:**
  - undecodable bytes for the column's charset;
  - an ENUM index 0 (v1 and v2), or an index above the definition;
  - a SET bit outside the definition;
  - an unsupported JSONB opaque type;
  - a snapshot value the schema-driven decoder cannot parse.
- **Rule:** the source fails the pipeline closed. No event is emitted for the row's transaction, and the source checkpoint stays before that transaction. The incident names the column, the MySQL type and the reason, never the value.

**Sink conversion failures (per sink, per event):**
- **Causes:** a canonical value that a sink's target type cannot hold exactly, e.g.:
  - BIGINT UNSIGNED above i64::MAX in long or number mode;
  - a zero date in a typed temporal field;
  - a label that is not a valid Avro enum symbol.
- **Rule:** the failure is detected before any record is produced (the Avro and Arrow converters never substitute).
  - **With a configured DLQ,** the sink may advance past the event only after the DLQ durably acknowledges the original event together with the conversion failure: column, MySQL type, target type and mapping version.
  - **Without a DLQ, or when the DLQ does not acknowledge,** the sink's checkpoint stays before the event and the pipeline halts with an incident.
  - Conversion failures are deterministic, so they are not retried in a loop. A sink never reports a poison row as handled unless the DLQ acknowledged it.
  - The incident never includes the value. The DLQ entry carries the event under the DLQ's own data policy.

## 7. Schema, fingerprint, changelog and migration

**Unconditional corrections** (rc.1, both mappings):
- YEAR 0000 no longer becomes 1900;
- JSON null no longer becomes SQL NULL;
- BINARY padding is kept;
- TEXT is no longer written as base64-wrapper text;
- charset decoding is applied;
- ENUM/SET emit labels, not indexes;
- DATE no longer becomes the epoch;
- BIT is no longer ASCII digits;
- the TIMESTAMP unit matches its declared type;
- snapshot numeric (and every other) value is typed;
- VARBINARY/BLOB rendering no longer depends on the data.

**Fingerprints and schemas:**
- **Source schema fingerprints** (the registry hash of the MySQL definition) do not change: the MySQL definition did not change. Activation proofs and checkpoints are unaffected.
- **The event encoding identity** is (source schema fingerprint, mapping version). Everything keyed by how an event is encoded includes the version: Avro schema cache keys and subjects, the S3 writer identity and encoding domain, and DLQ and replay records.
- **Every published output schema changes when its logical schema changes:**
  - **Avro:** schemas carry `deltaforge.type_mapping: v2` and the corrected types (TIMESTAMP micros, TIME long micros, BIT(1) boolean, BIT(n) fixed, labels). Their Schema Registry ids change. v2 subjects use a new name suffix, so incompatible changes are never checked against v1 subjects.
  - **Arrow/Parquet:** the schema metadata carries `deltaforge.type_mapping`, and the S3 encoding domain's `format_version` is bumped. New objects never share a domain, compaction or content identity with old ones, even where the Arrow type is unchanged but values were corrected (TEXT).
  - **Native JSON:** every event carries `source.type_mapping` (stamped at the source, section 5), mirrored as a Kafka header and in the S3 object metadata.

**Changelog and migration guide** (part of the implementation, required before rc.1):
- one breaking-correction entry per item above, with before/after examples;
- the v2 representation table;
- how to pin v1 and what it does and does not keep.

**Migration steps:**
- Avro and Parquet users re-snapshot: snapshots could not be delivered before.
- Consumers of native JSON update parsers for:
  - TIMESTAMP (number to RFC 3339 text);
  - JSON (embedded value to semantic string);
  - ENUM/SET (to labels);
  - TEXT (base64 wrapper to string);
  - binary (always `{"_base64":"..."}`);
  - BIGINT and BIGINT UNSIGNED (number to exact decimal string, unless `json_wide_integers: number`, whose 2^53 risk the guide states prominently);
  - BIT (number to `true`/`false` for BIT(1), fixed-width base64 bytes for BIT(n>1)).
- **Switching an existing pipeline:** the version is per event, so consumers see the switch on each event's `source.type_mapping`. A restart marker is only informational.

## 8. Separate blockers (their own reviewed changes)

### 8.1 Avro converter: fail closed

The converter never substitutes zero, epoch, empty text or another fallback.
- Exact conversion succeeds. Anything else fails before a record exists, with the section 6 incident and checkpoint rules.
- Today's fallbacks to remove: logical-type branches defaulting to 0; BYTES from non-strings taking the JSON text; numbers passed unchanged into timestamp fields of another unit. The int/long fallback was fixed in #147.
- **Tests** (unit and live, per family):
  - BIGINT UNSIGNED in both modes;
  - DATE and TIMESTAMP units and zero values;
  - BIT(1) and BIT(n);
  - ENUM/SET labels and invalid indexes;
  - TEXT in utf8mb4 and latin1, and undecodable bytes;
  - JSON null/object/number exactness.
  - Each corrected family gets a negative test that fails on the old converter.

### 8.2 S3 object keys: bounded (release blocker)

- **Problem:** durable keys embed the hex-encoded watermark (`wm-<hex>`). With a MySQL GTID position that is a path segment of about 450 bytes (449 and 453 in two runs), and RustFS and MinIO reject it (`400 InvalidArgument`). AWS caps keys at 1024 bytes, and fleet `gtid_executed` sets name many UUIDs.
- **Fix:** replace the component with a bounded deterministic identity, a versioned digest (for example `wm2-<sha256 of the canonical watermark encoding>`). Keep the full position in object metadata, the manifest and durable checkpoint state.
- **The design must prove:**
  - a retry produces the same key;
  - different positions do not collide under the stated model (canonical encoding plus SHA-256);
  - keys stay bounded for arbitrarily large GTID sets;
  - restart and replay remain idempotent;
  - existing `wm-` objects stay readable;
  - the old-to-new key migration is explicit;
  - both AWS S3 and file-backed stores (RustFS) work, with a test for each.

## 9. Implementation slices and required tests

Each slice is its own reviewed PR with a core gate. Slices 1 and 2 are independent. Slices 3 and 4 land together for parity. Slice 5 follows 3. Slice 6 completes the contract.

| # | Slice | Required tests |
|---|---|---|
| 1 | Avro converter fail-closed (8.1) | section 8.1 |
| 2 | S3 bounded keys (8.2) | section 8.2 proofs, RustFS and AWS-compatible |
| 3 | Canonical value model and CDC decoders: charset, TEXT, BINARY padding, ENUM index/label (index 0 fails closed in v1 and v2), SET, YEAR, JSON text from JSONB, temporal text, FLOAT f32, BIT bytes and boolean, BIGINT UNSIGNED policy | unit tests per type at the probe's boundary values; a declared `''` ENUM label versus index 0; JSONB rendering equal to MySQL text for the probe documents and an opaque-type set; each source decoding failure fails the pipeline closed with no event and no checkpoint advance |
| 4 | Snapshot schema-driven decoding: binary results, UTC session, the same decoders | **promote the probe to an asserting parity suite**: snapshot equals CDC per value, per sink, MINIMAL and FULL (core gate) |
| 5 | Typed sink mappings: Avro micros types, TIME, BIT boolean/fixed, labels; Arrow Duration, Timestamp(us), FixedSizeBinary; Parquet TEXT | the parity suite through Avro and Parquet; schema-registry id and Arrow metadata change checks; sink conversion failures: with a DLQ the sink advances only after the DLQ acknowledges; without one, or when the DLQ refuses, the checkpoint holds and the pipeline halts (no retry loop) |
| 6 | Per-event mapping version: stamping, batch split, replay/DLQ preservation, S3 and Avro identities, downgrade fail-closed, `type_mapping` config (default v2, v1 lossless-only), changelog and migration guide | a lagging optional sink replays v1 events after the pipeline switched to v2 and encodes them as v1; a mixed-version batch is split; DLQ and replay records keep their version; v1 and v2 are rendered from the same canonical event, and replay and DLQ persist the canonical form; an unsupported version fails closed; config tests (default v2, `json_wide_integers` default string, number mode failing above i64::MAX for BIGINT UNSIGNED, typed sinks unaffected); version present on every sink's output; docs |

## 10. Open items

DECIMAL with p > 38 in Parquet stays Utf8 (exact) for rc.1. Decimal256 is a later, separately versioned change.

ENUM error-sentinel emission is out of rc.1. If needed, it comes as a separately versioned, uniformly tagged ENUM representation.
