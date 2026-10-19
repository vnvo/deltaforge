# Canonical event model

Status: implemented (canonical-types slices 3-6, one feature; reviewer
rulings of 2026-10-11 and 2026-10-12). Extends
[MySQL canonical types](mysql-canonical-types.md) with the event model, the
PostgreSQL mapping, the processor boundary and the sink contract.

## 1. One representation

A row image (`Event::before`, `Event::after`) is a `CanonicalRow`. It has two
logical parts:

- **declared columns:** ordered `(column name, CanonicalValue)` pairs,
  governed exclusively by the source schema;
- **document extensions:** `name -> Document`, JSON documents a processor
  added to an event whose columns its source schema declares. They are not
  database columns. An extension name never equals a column name, and no
  extension name is a dotted path prefix of another (`a` and `a.b` conflict).

There is no JSON row image and no sidecar. Every consumer renders from the
canonical values.

`CanonicalValue` is a closed sum type. No two variants mean the same thing:

| Variant | Meaning |
|---|---|
| `Null` | SQL NULL |
| `Unchanged` | PostgreSQL unchanged TOAST value: the column was not sent. Legal only in a PostgreSQL update's after image |
| `Db(DbValue)` | a schema-typed database value (section 2) |
| `Opaque { source_type, text }` | a value of a source type without a canonical mapping: the source's deterministic text under a pinned session, with the type's identity (engine, name, OID, typmod) |
| `Document(Document)` | an untyped JSON document of a schema-less event (logical messages, outbox output, fields a processor added to a schema-less event). Valid JSON only; its canonical text sorts object keys at every level |
| `Bytes(Vec<u8>)` | arbitrary bytes without a column type (PostgreSQL logical-message content) |

A SQL JSON column is `Db(Json)`, never `Document`; a binary column is
`Db(Binary)` (or `Db(Bits)` for MySQL BIT), never `Bytes`.

## 2. Database values

| `DbValue` | Holds | Invariant |
|---|---|---|
| `Int { value, width }` / `UInt` | i64 / u64 and the declared width (8/16/24/32/64) | value fits the width |
| `Decimal` | exact decimal text, or NaN/+Infinity/-Infinity (PostgreSQL) | canonical text: no leading zeros, no `-0`, the scale as stored |
| `Float32` / `Float64` | IEEE value; non-finite values only from PostgreSQL | |
| `Bool`, `Text`, `Binary` | | binary keeps its padding (BINARY(n) is n bytes) |
| `Bits { len, bytes }` | MySQL BIT(len): ceil(len/8) big-endian bytes | no bit above len |
| `Enum { label, index }` | MySQL ENUM: label and 1-based index | index 1..N (index 0 is a decoding failure); PostgreSQL enums are `Opaque` |
| `Set { bitmask, members }` | MySQL SET | members in definition order, matching the bitmask |
| `Json` | the source's canonical JSON text (MySQL: `CAST(j AS CHAR)` text, rendered from JSONB in CDC) | valid JSON |
| `Date` | civil date (MySQL zero parts allowed), or +/-infinity | |
| `Time { micros, precision }` | MySQL signed duration or PostgreSQL time of day | the value fits its precision |
| `DateTime` | civil date-time with precision, MySQL zero, or +/-infinity | |
| `Timestamp` | UTC instant in microseconds with precision, MySQL zero, or +/-infinity | |
| `Year` | 0 or 1901..2155 | |
| `Uuid` | 16 bytes | |
| `Array { element_type, items }` | PostgreSQL array of a mapped builtin element type, one dimension level per nesting, lower bound 1 | items typed by the declared element type; a value with other lower bounds is a source decoding failure; arrays of an unmapped element type are `Opaque` as a whole |

Precision is `Fixed(0..=6)` (declared) or `Variable` (PostgreSQL without a
typmod: rendered with trailing fractional zeros removed).

## 3. Decoding

Each engine has one schema-driven decoder, shared by snapshot and CDC. The
declared column type (from the decoding schema version, never from a value)
decides the canonical value. A value that does not decode exactly is a
**source decoding failure**: the `source_decode_failed` incident (column,
type and table as evidence, never the value), and the source stops before
the value's transaction (with transaction-aligned batching, the default, no
event of it is delivered; the checkpoint stays before it).

- **MySQL:** CDC reads the binlog's stored values (the vendored parser
  returns structured temporal parts and keeps YEAR 0); the snapshot reads the
  binary protocol on a session pinned to `time_zone = '+00:00'` and
  `character_set_results = binary`, selecting ENUM and SET as `CAST(col AS UNSIGNED)` (the
  stored index and bitmask). Text decodes from the column's collation id
  (utf8mb4, utf8mb3, ascii, latin1 as MySQL's cp1252; any other charset fails
  closed). JSONB renders to MySQL's own text (key order, separators, escapes,
  shortest round-trip doubles with MySQL's fixed/exponent choice, opaque
  DECIMAL/temporal forms).
- **PostgreSQL:** both paths read each value's output-function text under
  one pinned session (`DateStyle 'ISO, YMD'`, `TimeZone UTC`, `IntervalStyle
  postgres`, `extra_float_digits 3`, `bytea_output hex`, `lc_monetary C`,
  `search_path pg_catalog`): CDC through pgoutput's text mode on a walsender
  session pinned before `START_REPLICATION`, the snapshot through
  `format('%s', col)` (the type's output function; NULL stays NULL) in one
  extended-protocol query with the existing binary identity columns. The
  tuple parser is strict (a truncated or unknown tuple fails closed). A
  key-only old image (`K`) holds only the key columns. Logical messages are a
  `content` column of `Bytes`; an outbox message (a JSON object by contract)
  is one `Document` column per field, or its bytes when it is not one.

A source decoding failure is deterministic: the source neither retries the
value (a snapshot table is not copied again in the same read view) nor
moves its checkpoint, so every restart meets it again until the value or
the table selection changes.

**Table identity.** A table is database + namespace + table: `source.db` is
the database, `source.schema` the PostgreSQL schema (absent for MySQL, whose
namespace is its database), identically in snapshot and change events.
Its typed form, `SourceRelationIdentity` (connector, source, database,
namespace, table), is the uniqueness key: caches, idempotency keys and
stored records use its injective canonical encoding (parts percent-escaped,
joined by `/`) or its storage-safe key segment (parts escaped to `[A-Za-z0-9_]`,
joined by `.`), Avro names its escaped parts. `namespace.table` is for
display, table selection and routing, and names a table to its own
source's schema registry, drift detector and sensor (all scoped to one
pipeline and so one database); it is never a key across sources. Object-key segments
too long for a backend (the 200-byte segment, 900-byte key bounds) use the
relation's domain-separated digest; the manifest keeps the readable
identity. A sink destination written by key (a ClickHouse table, an
Elasticsearch index) holds one relation, named in the immutable ownership
marker the sink creates it with; writes carry a fence the destination
enforces (see the sinks' destination ownership documentation), and a second
relation, another owner or an unmarked target is refused
(`sink_destination_conflict`) with the checkpoint held. PostgreSQL registers two schema shapes for a table (the
Relation-proven event schema of change events, the catalog schema of the
snapshot); both describe the same DDL to encoders identically (by type OID
and modifier, every column nullable), so a snapshot row and a change row of
one table encode under one schema.

## 4. Mapping versions

Every event carries `type_mapping` (`v1` or `v2`), stamped at the source
before batching, the replay journal or the DLQ. Every consumer renders the
event's own version. PostgreSQL sources refuse `v1` at configuration load
(there was no lossless legacy output to keep). `v1` differs from `v2` only in
native JSON, only for MySQL events: wide integers and BIT(n>1) as JSON
numbers, TIMESTAMP as epoch microseconds, TIME with six fractional digits,
DATETIME as `YYYY-MM-DD HH:MM:SS.ffffff`, FLOAT as the widened f64.

v2 native JSON renderings:

| Value | Native JSON |
|---|---|
| Int/UInt of width 64 | exact decimal string (`json_wide_integers: string`, default) or a number (`number`; UInt above i64::MAX is then a sink conversion failure) |
| other Int/UInt, Year | number |
| Decimal | string; NaN/Infinity as `"NaN"`, `"Infinity"`, `"-Infinity"` |
| Float32 / Float64 | shortest round-trip number (f32 digits for Float32); non-finite as the strings above |
| Bool | `true` / `false` |
| Text, Opaque | string |
| Binary, Bytes | `{"_base64":"..."}` |
| Bits | len 1: `true`/`false`; otherwise `{"_base64":"..."}` of ceil(len/8) bytes |
| Enum / Set | label / `"a,c"` |
| Json | string (canonical text) |
| Document, extension | embedded JSON |
| Date / Time / DateTime | `YYYY-MM-DD` / `[-]HH:MM:SS[.f]` / `YYYY-MM-DDTHH:MM:SS[.f]`; MySQL zero parts as MySQL writes them; `infinity`/`-infinity` |
| Timestamp | RFC 3339 UTC with `Z`; MySQL zero `0000-00-00 00:00:00`; infinities |
| Uuid | lowercase hyphenated text |
| Array | JSON array of the item renderings |
| Null | `null` |

Years outside 0000..9999 render with a sign and at least five digits (ISO
8601 expanded, astronomical numbering: 1 BC is `0000`). An `Unchanged` column
is omitted from the native row image and listed in `unchanged_columns`.

**Mixed versions** are an encoding detail inside each sink, never a
checkpoint or acknowledgement boundary: every sink renders each event with
its own version within one `send_batch` (one acknowledgement, one Kafka
transaction, the original checkpoint held on any failure). Version-keyed
identities keep versions apart: Avro subjects `<subject>-v1` / `-v2` (and the
schema's `deltaforge.type_mapping`), the S3 rolling writer key, the durable S3
encoding domain (one object per table and version, all under the batch's one
manifest entry), the Kafka `df-type-mapping` header.

## 5. Sinks

| Sink | Declared values | Document columns (schema-less events) | Extensions | Unchanged TOAST |
|---|---|---|---|---|
| Native JSON (Kafka, Redis, NATS, HTTP, S3 JSONL) | section 4 | embedded JSON | merged into the row (a collision with a column is a conversion failure) | omitted, listed in `unchanged_columns` |
| Avro, DDL schema | converted from canonical values to the field's Avro type exactly | refused in a declared column | sibling fields `before_extensions` / `after_extensions`: nullable `map<string>` of canonical JSON text, part of every schema (adding fields never registers a schema) | conversion failure |
| Avro, inferred schema | exact JSON (integers as numbers) | canonical JSON text | merged as canonical JSON text fields | conversion failure |
| Parquet | converted to the column's Arrow type exactly (Decimal128 never truncated; Timestamp micro; Duration for MySQL TIME; FixedSizeBinary for BIT(n)) | refused in a declared column | `before_extensions` / `after_extensions` Utf8 columns holding canonical JSON objects (null when none); a table column named `extensions` is refused | conversion failure |
| ClickHouse | exact RowBinary per column type (integers never wrapped, decimals never truncated) | refused | one `extensions_column` (default `_extensions`, `Nullable(String)`): the delivered image's extensions as a canonical JSON object; a source column of that name (or a meta column's) is refused before any row of the batch is delivered | conversion failure |
| Elasticsearch | by canonical type (binary as base64 string, integers exact, temporal text) | embedded JSON in undeclared fields; refused in declared ones | merged into the document (a collision with a column is refused) | refused |

`Bytes` encode only as binary targets (Avro bytes/fixed, Arrow
Binary/FixedSizeBinary, ClickHouse String), exactly; a `Bytes` value for a
non-binary column fails. A column a row carries but its schema version does
not define is refused by every typed sink, never dropped. An empty extension
map is absent from native JSON and null in typed sinks, always.

**PostgreSQL limitation:** an unchanged TOAST value in a typed relational
output (Avro, Parquet, ClickHouse, Elasticsearch) is an explicit conversion
failure, never NULL or empty. PostgreSQL Avro and Parquet output is not
described as generally supported until the live parity suite exercises
realistic TOAST updates.

## 6. Durable form

The replay journal (envelope version 2) and the DLQ keep the lossless
canonical form: each value a tagged JSON object (integers as decimal text,
floats as IEEE bit patterns, bytes base64, documents and extensions as
canonical JSON text), each row `{"c": [[name, value], ...], "x": {extensions}}`
(`x` only when there are extensions), and the event's `type_mapping`. A
replay job's preflight decodes every envelope of its range before delivering
any event or moving its cursor: a version-1 capture (untyped JSON events) is
refused there, whole, with the format version, the reason (it predates
canonical values; types cannot be inferred) and the operator action. DLQ
records of earlier versions stay readable and are not converted.

## 7. Processor boundary

Processors receive the batch's **schema context**: before any processor
runs, the runner resolves every distinct schema reference (table,
fingerprint, sequence) of the batch to the declared column types of exactly
that registered version, through the source's own type mapping. A missing
version, a fingerprint or sequence conflict, or a value its version does not
admit fails the batch before any processor mutates it. Resolved versions are
immutable and cached by their exact reference (bounded, oldest evicted
first). Events carry no column metadata. Processors depend only on the core
schema contract (`canonical::schema`); sources implement the interpretation.

- **JavaScript:** scripts see each event's native rendering for its version,
  with every 64-bit integer (and v1 BIT number) as a decimal string, and the
  extensions merged in. On return: a value equal to its original rendering
  gets its original canonical value back exactly; any other value of a
  declared column (changed, set on a NULL or unchanged TOAST column, a
  declared column the image lacked) parses exactly as the declared type, ENUM
  labels and SET members against their declared symbols, or the batch fails;
  a removed field is removed; a field the schema does not declare is an
  extension (on a schema-less event, a `Document` column). Missing or
  conflicting schema context for a database column fails the batch; it is
  never downgraded to a document. Lineage rules are unchanged.
- **Filter:** predicates read the path view (integers exact numbers, compared
  exactly; a path descends into a JSON column; extensions visible); a
  predicate on an unchanged TOAST column fails the batch; `changed` treats it
  as unchanged.
- **Flatten:** document columns expand into columns and extensions into
  extensions; declared values (a JSON column included) are leaves.
- **Outbox:** the transformed row is explicit document data (a JSON object
  payload becomes one `Document` column per field; any other payload one
  column; bytes stay bytes); the outbox table's schema reference is removed;
  `type_mapping`, lineage and event identity are kept.
- **Routing templates** resolve against the path view.
- **Schema sensing** inspects document structures only (document columns and
  extensions); declared columns have an authoritative schema and an event's
  schema reference is never rewritten. **Drift detection** compares each
  declared column's canonical kind with its declared type.
